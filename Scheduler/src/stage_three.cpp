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
	else if(lhs.cores == rhs.cores && lhs.memory >= rhs.memory && lhs.disk >= rhs.disk && lhs != rhs) return true;
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
		intmax_t current_time = static_cast<intmax_t>(job.submit_time);
		resource_info current_util = server->avail_resc;
		std::vector<schd_info> remaining_jobs;
		for(auto s = 0; s < server->num_jobs; ++s) {
			auto schd_job(server->jobs[s]);
			//if(schd_job.start_time != -1 && schd_job.start_time + schd_job.est_runtime < current_time) schd_job.est_runtime = 1 + current_time - schd_job.start_time;
			remaining_jobs.push_back(schd_job);
		}

		std::sort(ITER(remaining_jobs), [](schd_info lhs, schd_info rhs) { return lhs.job_id < rhs.job_id; });
		
		// run a simulation of the currently allocated jobs until we hit a time when there are enough resources available to run the new one, then return that resource quantity and the time
		while(!remaining_jobs.empty()) {
			remaining_jobs.erase(std::remove_if(ITER(remaining_jobs), [current_time](schd_info arg) { return arg.start_time != -1 && arg.start_time + arg.est_runtime <= current_time; }), remaining_jobs.end());
			current_util = RESC_MIN;
			size_t waiting_jobs = 0;
			for(schd_info schd_job : remaining_jobs) {
				if(schd_job.start_time == -1) waiting_jobs++;
				else current_util = current_util + schd_job.req_resc;
			}
			if(waiting_jobs == 0 && (current_util + job.req_resc) <= server->type->max_resc) break;
			bool scheduled_new_job = false;
			do {
				scheduled_new_job = false;
				for(auto &schd_job : remaining_jobs) {
					if(schd_job.start_time != -1) continue;
					else if((current_util + schd_job.req_resc) <= server->type->max_resc) {
						current_util = current_util + schd_job.req_resc;
						schd_job.start_time = current_time;
						scheduled_new_job = true;
					}
				}
			} while(scheduled_new_job);
			intmax_t next_finished_time = std::numeric_limits<intmax_t>::max();
			for(auto schd_job : remaining_jobs) {
				if(schd_job.start_time != -1) next_finished_time = std::min(schd_job.start_time + static_cast<intmax_t>(schd_job.est_runtime*2), next_finished_time);
			}
			current_time = next_finished_time;
		}
		return std::pair<intmax_t, resource_info>(current_time, resc_diff(current_util, server->type->max_resc));
	}
}

// general idea: schedule job on available servers, then on offline servers, then on busy servers with descending quantity of jobs
server_info *stage_three(system_config* config, job_info job) {
	server_info *cur_server = nullptr;
	resource_info cur_margin = RESC_MIN;
	intmax_t cur_avail_time = std::numeric_limits<intmax_t>::max();
	size_t cur_waiting = std::numeric_limits<size_t>::max();
	size_t cur_pending = std::numeric_limits<size_t>::max();
	bool cur_can_run_now = false;

	for(auto s = 0; s < config->num_servers; ++s) { 
		auto *server = &config->servers[s];
		auto est_stat = est_avail_stat(server, job);
		intmax_t avail_time = est_stat.first;
		resource_info avail_resc = est_stat.second;
		if(!job.can_run(avail_resc)) continue; // if we couldn't find any time or any resource value where the job could run on the server, skip it
		auto new_margin = resc_diff(avail_resc, job.req_resc); // how much the 
		auto waiting_jobs = num_waiting_jobs(server); // number of jobs allocated and not running yet
		auto pending_jobs = server->num_jobs; // number of jobs currently allocated to a server
		bool can_run_now = job.can_run(server->avail_resc); // handles cases where jobs have already exceeded their estimated run-time, and are running
		// if there are no waiting jobs on the server and the next best has waiting jobs
		// if there are no running jobs on the server and the next best has running jobs and can't immediately run the job
		if(((waiting_jobs == 0 && cur_waiting > 0) || (can_run_now && !cur_can_run_now)) // ALWAYS boot a new server instead of waiting if possible
			|| (((cur_waiting == 0) == (waiting_jobs == 0) && cur_can_run_now == can_run_now)
			&& (avail_time < cur_avail_time 
			|| (avail_time == cur_avail_time 
			&& (compare_margins(new_margin, cur_margin)
			|| (new_margin.cores == cur_margin.cores//new_fitness == cur_fitness
			&& (waiting_jobs < cur_waiting
			|| (waiting_jobs == cur_waiting
			&& (pending_jobs < cur_pending
			|| (pending_jobs == cur_pending
			&& server->type->rate <= cur_server->type->rate
			)))))))))) // trust me this is the best it'll look
		{
			cur_avail_time = avail_time;
			cur_server = server;
			cur_waiting = waiting_jobs;
			cur_margin = new_margin;
			cur_pending = pending_jobs;
			cur_can_run_now = can_run_now;
		}
	}
	return cur_server;
}