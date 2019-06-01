#include "stage_three.h"

#include <map>
#include <set>
#include <algorithm>
#include <functional>
#include <numeric>
#include <limits>
#include <cstdint>
#include <iostream>

#define ITER(ARG) ARG.begin(),ARG.end()

constexpr resource_info RESC_MAX {
	std::numeric_limits<uintmax_t>::max(),
	std::numeric_limits<uintmax_t>::max(),
	std::numeric_limits<uintmax_t>::max()
};

constexpr resource_info RESC_MIN { 0, 0, 0 };

resource_info resc_diff(const resource_info &lhs, const resource_info &rhs) noexcept {
	return resource_info{
		std::max(lhs.cores,rhs.cores) - std::min(lhs.cores,rhs.cores),
		std::max(lhs.memory,rhs.memory) - std::min(lhs.memory,rhs.memory),
		std::max(lhs.disk,rhs.disk) - std::min(lhs.disk,rhs.disk)
	};
};

bool wf_compare_margins(const resource_info &lhs, const resource_info &rhs) noexcept {
	if(lhs.cores > rhs.cores) return true;
	else if(lhs.cores == rhs.cores && lhs.memory >= rhs.memory && lhs.disk >= rhs.disk && lhs != rhs) return true;
	else return false;
}

bool bf_compare_margins(const resource_info &lhs, const resource_info &rhs) noexcept {
	if(lhs.cores < rhs.cores) return true;
	else if(lhs.cores == rhs.cores && lhs.memory <= rhs.memory && lhs.disk <= rhs.disk && lhs != rhs) return true;
	else return false;
}

size_t num_waiting_jobs(const server_info *server) noexcept {
	size_t total = 0;
	for(auto s = 0; s < server->num_jobs; ++s) {
		schd_info schd_job = server->jobs[s];
		if(schd_job.start_time == -1) total++;
	}
	return total;
}

/*
Runs look-ahead simulation on the jobs currently allocated to the server,
.. up until there would be enough resources free to run the new job.
Returns the time that this happens, and the resources available at that point.
*/
std::tuple<intmax_t, size_t, resource_info> est_avail_stat(const server_info *server, const job_info &job) noexcept {
	if(!job.can_run(server->type->max_resc) || server->state == SS_UNAVAILABLE) return std::tuple<intmax_t, size_t, resource_info>(std::numeric_limits<intmax_t>::max(), 0, RESC_MIN);
	else if(job.can_run(server->avail_resc)) return std::tuple<intmax_t, size_t, resource_info>(std::max(server->avail_time, static_cast<intmax_t>(job.submit_time)), 0, server->avail_resc);
	else {
		intmax_t current_time = static_cast<intmax_t>(job.submit_time);
		if(server->state == SS_BOOTING || server->state == SS_INACTIVE) current_time += server->type->bootTime; // NEW
		resource_info current_util = server->avail_resc;
		std::vector<schd_info> remaining_jobs;
		for(auto s = 0; s < server->num_jobs; ++s) {
			auto schd_job(server->jobs[s]);
			remaining_jobs.push_back(schd_job);
		}

		std::sort(ITER(remaining_jobs), [](schd_info lhs, schd_info rhs) { return lhs.job_id < rhs.job_id; });
		size_t waiting_jobs = 0;
		// run a simulation of the currently allocated jobs until we hit a time when there are enough resources available to run the new one, then return that resource quantity and the time
		while(!remaining_jobs.empty()) {
			remaining_jobs.erase(std::remove_if(ITER(remaining_jobs), [current_time](schd_info arg) { return arg.start_time != -1 && arg.start_time + arg.est_runtime <= current_time; }), remaining_jobs.end());
			current_util = RESC_MIN;
			//bool has_waiting_job;
			waiting_jobs = 0;

			for(schd_info schd_job : remaining_jobs) {
				if(schd_job.start_time == -1) waiting_jobs++;
				else current_util = current_util + schd_job.req_resc;
			}
			for(auto &schd_job : remaining_jobs) {
				if(schd_job.start_time == -1 && (current_util + schd_job.req_resc) <= server->type->max_resc) {
					current_util = current_util + schd_job.req_resc;
					schd_job.start_time = current_time;
					waiting_jobs--;
				}
			}

			if((current_util + job.req_resc) <= server->type->max_resc) break;// if the job can run now, it gets run

			intmax_t next_finished_time = std::numeric_limits<intmax_t>::max();
			for(auto schd_job : remaining_jobs) {
				if(schd_job.start_time != -1) next_finished_time = std::min(schd_job.start_time + static_cast<intmax_t>(schd_job.est_runtime*2), next_finished_time);
			}
			current_time = next_finished_time;
		}
		return std::tuple<intmax_t, size_t, resource_info>(current_time, waiting_jobs, resc_diff(current_util, server->type->max_resc));
	}
}

enum search_mode {FF = 0, WF = 1, BF = 2};

// general idea: schedule job on available servers, then on offline servers, then on busy servers with descending quantity of jobs
server_info *stage_three(system_config* config, job_info job) {
	server_info *cur_server = nullptr;
	resource_info cur_margin = RESC_MIN;
	intmax_t cur_avail_time = std::numeric_limits<intmax_t>::max();
	size_t cur_waiting = std::numeric_limits<size_t>::max();
	size_t cur_pending = std::numeric_limits<size_t>::max();
	size_t cur_delayed = std::numeric_limits<size_t>::max();
	bool cur_can_run_now = false;
	search_mode cur_mode = FF;

	for(auto s = 0; s < config->num_servers; ++s) { 
		auto *server = &config->servers[s];
		auto est_stat = est_avail_stat(server, job);
		intmax_t avail_time = std::get<0>(est_stat);
		size_t delayed_jobs = std::get<1>(est_stat);
		resource_info avail_resc = std::get<2>(est_stat);
		if(!job.can_run(avail_resc)) continue; // if we couldn't find any time or any resource value where the job could run on the server, skip it
		auto new_margin = resc_diff(avail_resc, job.req_resc); // how much the
		auto waiting_jobs = num_waiting_jobs(server); // number of jobs allocated and not running yet
		auto pending_jobs = server->num_jobs; // number of jobs currently allocated to a server
		bool can_run_now = job.can_run(server->avail_resc) && (server->state == SS_IDLE || server->state == SS_ACTIVE); // handles cases where jobs have already exceeded their estimated run-time, and are running
		//TODO: maybe look at servers that are booting?
		search_mode new_mode = FF;
		if(job.can_run(server->avail_resc) && (server->state == SS_IDLE || server->state == SS_ACTIVE) && waiting_jobs == 0) new_mode = BF;
		else if(server->state == SS_INACTIVE) new_mode = WF;

		if(new_mode < cur_mode) continue;
		else if(cur_mode == new_mode) switch(cur_mode) {
			case BF:
				if(bf_compare_margins(new_margin, cur_margin)) break;
				else if(bf_compare_margins(cur_margin, new_margin)) continue;
				else if(server->type->rate < cur_server->type->rate) break;
				else continue;
			case WF:
				if(wf_compare_margins(new_margin, cur_margin)) break;
				else if(wf_compare_margins(cur_margin, new_margin)) continue;
				if(server->type->bootTime < cur_server->type->bootTime) break; // calculate a ratio for boot time vs server size based on current job resource requirement
				else if(server->type->bootTime > cur_server->type->bootTime) continue;
				if(server->type->rate < cur_server->type->rate) break;
				else continue;
			case FF: // only switching on pending gives better results for long simulations, only switching on time gives good results for short ones
				intmax_t cur_worst = cur_avail_time + (1 + cur_delayed) * job.est_runtime;
				intmax_t new_worst = avail_time + (1 + delayed_jobs) * job.est_runtime;
				// compare by weighted available time
				if(new_worst <= cur_avail_time) break;
				else if(avail_time >= cur_worst) continue;
				// compare by best-fit
				if(bf_compare_margins(new_margin,cur_margin)) break;
				else if(bf_compare_margins(cur_margin,new_margin)) continue;
				// compare by available time
				if(avail_time < cur_avail_time) break;
				else if(avail_time > cur_avail_time) continue;
				// compare by potentially delayed jobs
				if(delayed_jobs < cur_delayed) break;
				else if(delayed_jobs > cur_delayed) continue;
				// compare by cost
				if(server->type->rate < cur_server->type->rate)break;
				continue;
		}

		cur_server = server;
		cur_margin = new_margin;
		cur_avail_time = avail_time;
		cur_waiting = waiting_jobs;
		cur_mode = new_mode;
		cur_delayed = delayed_jobs;
	}
	return cur_server;
}