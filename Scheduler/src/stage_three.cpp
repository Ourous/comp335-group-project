#include "stage_three.h"

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
constexpr resource_info RESC_MIN{ 0, 0, 0 };

resource_info resc_diff(const resource_info &lhs, const resource_info &rhs) noexcept {
	return resource_info{
		std::max(lhs.cores,rhs.cores) - std::min(lhs.cores,rhs.cores),
		std::max(lhs.memory,rhs.memory) - std::min(lhs.memory,rhs.memory),
		std::max(lhs.disk,rhs.disk) - std::min(lhs.disk,rhs.disk)
	};
};

bool wf_compare_margins(const resource_info &lhs, const resource_info &rhs) noexcept {
	if(lhs.cores > rhs.cores) return true;
	else return lhs > rhs;
}

bool bf_compare_margins(const resource_info &lhs, const resource_info &rhs) noexcept {
	if(lhs.cores < rhs.cores) return true;
	else return lhs < rhs;
}

bool has_zeroed_resc(const resource_info &resc) noexcept {
	return resc.cores == 0 || resc.memory == 0 || resc.disk == 0;
}

bool all_resc_larger(const resource_info &lhs, const resource_info &rhs) noexcept {
	return lhs.cores > rhs.cores && lhs.memory > rhs.memory && lhs.disk > rhs.disk;
}

size_t num_waiting_jobs(const server_info *server) noexcept {
	size_t total = 0;
	for(auto s = 0; s < server->num_jobs; ++s) {
		if(server->jobs[s].start_time == -1) total++;
	}
	return total;
}

/*
Runs look-ahead simulation on the jobs currently allocated to the server,
.. up until there would be enough resources free to run the new job.
Returns the time that this happens, and the resources available at that point.
*/
std::tuple<intmax_t, size_t, resource_info> est_avail_stat(const server_info *server, const job_info &job) noexcept {
	if(!job.can_run(server->type->max_resc) || server->state == SS_UNAVAILABLE) return std::make_tuple(std::numeric_limits<intmax_t>::max(), 0, RESC_MIN);
	else if(job.can_run(server->avail_resc)) return std::make_tuple(std::max(server->avail_time, static_cast<intmax_t>(job.submit_time)), 0, server->avail_resc);
	else {
		intmax_t current_time = static_cast<intmax_t>(job.submit_time);
		if(server->state == SS_BOOTING || server->state == SS_INACTIVE) current_time += server->type->bootTime;
		std::vector<schd_info> remaining_jobs(server->jobs, server->jobs + sizeof(schd_info)*server->num_jobs);

		resource_info current_util = resc_diff(server->type->max_resc, server->avail_resc);
		size_t waiting_jobs = 0;
		// run a simulation of the currently allocated jobs until we hit a time when there are enough resources available to run the new one, then return that resource quantity and the time
		while(!remaining_jobs.empty()) {
			remaining_jobs.erase(std::remove_if(remaining_jobs.begin(), remaining_jobs.end(), [current_time](schd_info arg) { return ~arg.start_time && arg.start_time + arg.est_runtime <= current_time; }), remaining_jobs.end());
			current_util = RESC_MIN;
			waiting_jobs = 0;

			for(auto schd_job : remaining_jobs) {
				if(~schd_job.start_time) current_util = current_util + schd_job.req_resc;
			}
			for(auto &schd_job : remaining_jobs) {
				if(!~schd_job.start_time){
					if((current_util + schd_job.req_resc) <= server->type->max_resc) {
						current_util = current_util + schd_job.req_resc;
						schd_job.start_time = current_time;
					} else if(!(schd_job.req_resc <= job.req_resc)) waiting_jobs++; // NOT the same as >
				}
			}

			if((current_util + job.req_resc) <= server->type->max_resc) break;// if the job can run now, it gets run

			intmax_t next_finished_time = std::numeric_limits<intmax_t>::max();
			for(auto schd_job : remaining_jobs) {
				if(~schd_job.start_time) next_finished_time = std::min(schd_job.start_time + static_cast<intmax_t>(schd_job.est_runtime), next_finished_time);
			}
			current_time = next_finished_time;
		}
		return std::make_tuple(current_time, waiting_jobs, resc_diff(current_util, server->type->max_resc));
	}
}

size_t inactive_of_type(const system_config* config, const server_type* type) noexcept {
	size_t inactive = 0;
	for(auto s = 0; s < type->limit; ++s) {
		if(start_of_type(config, type)[s].state == SS_INACTIVE) inactive++;
	}
	return inactive;
}

enum search_mode {SM_PREDICTIVE = 0, SM_START_NEW = 1, SM_BEST_FIT = 2};

// general idea: schedule job on available servers, then on offline servers, then on busy servers with descending quantity of jobs
server_info *predictive_fit(system_config* config, job_info job) {
	server_info *cur_server = nullptr;
	resource_info cur_margin = RESC_MAX;
	intmax_t cur_avail_time = std::numeric_limits<intmax_t>::max();
	size_t cur_delayed = std::numeric_limits<size_t>::max();
	search_mode cur_mode = SM_PREDICTIVE;

	for(auto s = 0; s < config->num_servers; ++s) { 
		auto *new_server = &config->servers[s];
		auto est_stat = est_avail_stat(new_server, job);
		intmax_t new_avail = std::get<0>(est_stat);
		size_t new_delayed = std::get<1>(est_stat);
		resource_info avail_resc = std::get<2>(est_stat);
		if(!job.can_run(avail_resc)) continue; // if we couldn't find any time or any resource value where the job could run on the server, skip it
		auto new_margin = resc_diff(avail_resc, job.req_resc); // how much the
		auto waiting_jobs = num_waiting_jobs(new_server); // number of jobs allocated and not running yet
		//TODO: maybe look at servers that are booting?
		search_mode new_mode = SM_PREDICTIVE;
		if(job.can_run(new_server->avail_resc) && (new_server->state == SS_IDLE || new_server->state == SS_ACTIVE) && waiting_jobs == 0) new_mode = SM_BEST_FIT;
		else if(new_server->state == SS_INACTIVE) new_mode = SM_START_NEW;

		if(new_mode < cur_mode) continue;
		else if(cur_mode == new_mode && cur_server != nullptr) switch(cur_mode) {
			case SM_BEST_FIT:
				// compare by best-fit
				if(bf_compare_margins(new_margin, cur_margin)) break;
				else if(bf_compare_margins(cur_margin, new_margin)) continue;

				// compare by cost
				if(new_server->type->rate < cur_server->type->rate) break;
				continue;

			case SM_START_NEW:

				// start large servers but don't take the last one
				if(all_resc_larger(new_margin, cur_server->type->max_resc) && inactive_of_type(config, new_server->type) > 2) break;
				else if(all_resc_larger(cur_margin, new_server->type->max_resc) && inactive_of_type(config, cur_server->type) > 2) continue;
					
				//if(bf_compare_margins(new_margin, cur_margin)) break;
				//else if(bf_compare_margins(cur_margin, new_margin)) continue;
				if(new_margin.cores < cur_margin.cores || new_margin < cur_margin) break;
				else if(new_margin.cores > cur_margin.cores || new_margin > cur_margin) continue;


				// if possible, take the one with lower bootup time
				if(new_server->type->bootTime < cur_server->type->bootTime) break;
				else if(new_server->type->bootTime > cur_server->type->bootTime) continue;
				// compare by cost
				if(new_server->type->rate < cur_server->type->rate) break;
				continue;

			case SM_PREDICTIVE: // only switching on pending gives better results for long simulations, only switching on time gives good results for short ones
				intmax_t cur_worst = cur_avail_time + (1 + cur_delayed) * job.est_runtime;
				intmax_t new_worst = new_avail + (1 + new_delayed) * job.est_runtime;
				// compare by weighted available time
				if(new_worst <= cur_avail_time) break;
				else if(new_avail >= cur_worst) continue;

				// compare by available time
				if(new_avail < cur_avail_time) break;
				else if(new_avail > cur_avail_time) continue;

				// compare by potentially delayed jobs
				if(new_delayed < cur_delayed) break;
				else if(new_delayed > cur_delayed) continue;
				
				// step forward in time and perform best-fit as usual
				if((new_delayed == 0 && cur_delayed == 0) || (has_zeroed_resc(new_margin) && has_zeroed_resc(cur_margin))) {
					//if(bf_compare_margins(new_margin, cur_margin)) break;
					//else if(bf_compare_margins(cur_margin, new_margin)) continue;
					if(new_margin.cores < cur_margin.cores || new_margin < cur_margin) break;
					else if(new_margin.cores > cur_margin.cores || new_margin > cur_margin) continue;
				} else { // try to leave resources to run the delayed jobs
					//if(wf_compare_margins(new_margin, cur_margin)) break;
					//else if(wf_compare_margins(cur_margin, new_margin)) continue;
					if(new_margin.cores > cur_margin.cores || new_margin > cur_margin) break;
					else if(new_margin.cores < cur_margin.cores || new_margin < cur_margin) continue;
				}

				// compare by cost
				if(new_server->type->rate < cur_server->type->rate)break;
				continue;
		}// else if(new_mode > cur_mode) s = -1; // restart loop at beginning
		
		// update current selection
		cur_server = new_server;
		cur_margin = new_margin;
		cur_avail_time = new_avail;
		cur_mode = new_mode;
		cur_delayed = new_delayed;
	}
	return cur_server;
}