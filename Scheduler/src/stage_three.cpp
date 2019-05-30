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

constexpr resource_info RESC_MAX = resource_info{
	std::numeric_limits<uintmax_t>::max(),
	std::numeric_limits<uintmax_t>::max(),
	std::numeric_limits<uintmax_t>::max()
};

constexpr resource_info RESC_MIN = resource_info{
	0, 0, 0
};

resource_info resc_diff(const resource_info &lhs, const resource_info &rhs) noexcept {
	return resource_info{
		std::max(lhs.cores,rhs.cores) - std::min(lhs.cores,rhs.cores),
		std::max(lhs.memory,rhs.memory) - std::min(lhs.memory,rhs.memory),
		std::max(lhs.disk,rhs.disk) - std::min(lhs.disk,rhs.disk)
	};
};

bool compare_margins(const resource_info &lhs, const resource_info &rhs) noexcept {
	if(lhs.cores > rhs.cores) return true;
	else if(lhs.cores == rhs.cores && lhs.memory >= rhs.memory && lhs.disk >= rhs.disk) return true;
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

std::pair<intmax_t, resource_info> est_avail_stat(const server_info *server, const job_info &job) noexcept {
	if(!job.can_run(server->type->max_resc) || server->state == SS_UNAVAILABLE) return std::pair<intmax_t, resource_info>(std::numeric_limits<intmax_t>::max(), RESC_MIN);
	else if(job.can_run(server->avail_resc)) return std::pair<intmax_t, resource_info>(server->avail_time, server->avail_resc);
	else {
		std::vector<schd_info> remaining_jobs;
		for(auto s = 0; s < server->num_jobs; ++s) {
			remaining_jobs.push_back(server->jobs[s]);
		}
		std::sort(ITER(remaining_jobs), [](schd_info lhs, schd_info rhs) { return lhs.job_id < rhs.job_id; });
		intmax_t current_time = static_cast<intmax_t>(job.submit_time);
		resource_info current_util = server->avail_resc;
		// add the first estimated finished job each time until we have enough resources, then return the resultant time
		while(!remaining_jobs.empty()) {
			remaining_jobs.erase(std::remove_if(ITER(remaining_jobs), [current_time](schd_info arg) { return arg.start_time != -1 && arg.start_time + arg.est_runtime <= current_time; }), remaining_jobs.end());
			current_util = RESC_MIN;
			size_t waiting_jobs = 0;
			for(schd_info schd_job : remaining_jobs) {
				if(schd_job.start_time == -1) waiting_jobs++;
				else current_util = current_util + schd_job.req_resc;
			}
			if(waiting_jobs == 0 && (current_util + job.req_resc) <= server->type->max_resc) break;
			while(true) {
				bool scheduled_new_job = false;
				for(auto &schd_job : remaining_jobs) {
					if(schd_job.start_time != -1) continue;
					else if((current_util + schd_job.req_resc) <= server->type->max_resc) {
						current_util = current_util + schd_job.req_resc;
						schd_job.start_time = current_time;
						scheduled_new_job = true;
					}
				}
				if(!scheduled_new_job) break;
			}
			intmax_t next_finished_time = std::numeric_limits<intmax_t>::max();
			for(auto schd_job : remaining_jobs) {
				if(schd_job.start_time != -1) next_finished_time = std::min(schd_job.start_time + static_cast<intmax_t>(schd_job.est_runtime*2), next_finished_time);
			}
			current_time = next_finished_time;
		}
		//if(!job.can_run(current_util))
		return std::pair<intmax_t, resource_info>(current_time, resc_diff(current_util, server->type->max_resc));
	}
}

// general idea: prioritize first available server, then the server least impacted by the job (either maxed out or way oversized)
server_info *stage_three(system_config* config, job_info job) {
	server_info *wf_server = nullptr;
	intmax_t wf_fitness = std::numeric_limits<intmax_t>::min();
	intmax_t wf_avail_time = std::numeric_limits<intmax_t>::max();
	size_t wf_waiting = std::numeric_limits<size_t>::max();
	resource_info wf_margin = RESC_MIN;

	// maybe also look at parsing lstj?

	for(auto s = 0; s < config->num_servers; ++s) { // ALWAYS boot a new server instead of waiting if possible
		auto *server = &config->servers[s];
		auto est_stat = est_avail_stat(server, job);
		intmax_t avail_time = est_stat.first;
		resource_info avail_resc = est_stat.second;
		if(!job.can_run(avail_resc)) continue;
		auto new_fitness = job.fitness(avail_resc);
		auto new_margin = resc_diff(avail_resc, job.req_resc);
		auto waiting_jobs = num_waiting_jobs(server);
		if(waiting_jobs == 0 && wf_waiting > 0) {
			wf_avail_time = avail_time;
			wf_fitness = new_fitness;
			wf_server = server;
			wf_waiting = waiting_jobs;
			wf_margin = new_margin;
		} else if((wf_waiting == 0) == (waiting_jobs == 0) && (avail_time < wf_avail_time || (avail_time == wf_avail_time && (compare_margins(new_margin, wf_margin) || (new_fitness == wf_fitness && waiting_jobs <= wf_waiting))))) {
			wf_avail_time = avail_time;
			wf_fitness = new_fitness;
			wf_server = server;
			wf_waiting = waiting_jobs;
			wf_margin = new_margin;
		}
	}
	return wf_server;
}