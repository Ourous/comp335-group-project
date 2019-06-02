#include "stage_three.h"

#include <algorithm>
#include <functional>
#include <numeric>
#include <limits>
#include <cstdint>
#include <iostream>
#include <cstdio>

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

bool has_zeroed_resc(const resource_info &resc) noexcept {
	return resc.cores == 0 || resc.memory == 0 || resc.disk == 0;
}

bool all_resc_larger(const resource_info &lhs, const resource_info &rhs) noexcept {
	return lhs.cores > rhs.cores && lhs.memory > rhs.memory && lhs.disk > rhs.disk;
}

size_t waiting_jobs(const server_info *server) noexcept {
	size_t total = 0;
	for(auto s = 0; s < server->num_jobs; ++s) {
		if(!~server->jobs[s].start_time) total++;
	}
	return total;
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
	intmax_t cur_avail = std::numeric_limits<intmax_t>::max();
	size_t cur_delayed = std::numeric_limits<size_t>::max();
	search_mode cur_mode = SM_PREDICTIVE;

	for(auto s = 0; s < config->num_servers; ++s) {

		auto *new_server = &config->servers[s];

		if(!job.can_run(new_server->type->max_resc) || new_server->state == SS_UNAVAILABLE) continue;

		intmax_t new_avail = new_server->avail_time;
		size_t new_delayed = 0;
		resource_info new_margin;
		search_mode new_mode = SM_PREDICTIVE;

		if(job.can_run(new_server->avail_resc) && waiting_jobs(new_server) == 0) {

			new_margin = resc_diff(new_server->avail_resc, job.req_resc);
			new_mode = new_server->state == SS_INACTIVE ? SM_START_NEW : SM_BEST_FIT;

		} else if(cur_mode == SM_PREDICTIVE) { // avoid doing work that we don't need to

			if(new_server->state == SS_ACTIVE) new_avail = static_cast<intmax_t>(job.submit_time);

			std::vector<schd_info> pending_jobs;
			for(auto j = 0; j < new_server->num_jobs; ++j) {
				pending_jobs.push_back(new_server->jobs[j]);
			}

			resource_info new_util = resc_diff(new_server->type->max_resc, new_server->avail_resc);

			// run a simulation of the currently allocated jobs until we hit a time when there are enough resources available to run the new one, then return that resource quantity and the time
			while(!pending_jobs.empty()) {

				pending_jobs.erase(std::remove_if(pending_jobs.begin(), pending_jobs.end(), [new_avail](schd_info arg) { return ~arg.start_time && arg.start_time + arg.est_runtime <= new_avail; }), pending_jobs.end());

				new_util = RESC_MIN;
				new_delayed = 0;

				for(auto schd_job : pending_jobs) {
					if(~schd_job.start_time) new_util = new_util + schd_job.req_resc;
				}

				for(auto &schd_job : pending_jobs) {
					if(!~schd_job.start_time) {
						if((new_util + schd_job.req_resc) <= new_server->type->max_resc) {
							new_util = new_util + schd_job.req_resc;
							schd_job.start_time = new_avail;
						} else if(!(schd_job.req_resc <= job.req_resc)) new_delayed++; // NOT the same as >
					}
				}

				if((new_util + job.req_resc) <= new_server->type->max_resc) break;// if the job can run now, it gets run

				intmax_t next_finished_time = std::numeric_limits<intmax_t>::max();
				for(auto schd_job : pending_jobs) {
					if(~schd_job.start_time) next_finished_time = std::min(schd_job.start_time + static_cast<intmax_t>(schd_job.est_runtime), next_finished_time);
				}

				new_avail = next_finished_time;
			}

			new_margin = resc_diff(new_util + job.req_resc, new_server->type->max_resc);
		}

		if(new_mode < cur_mode) continue;
		else if(cur_mode == new_mode && cur_server != nullptr) switch(cur_mode) {

			case SM_BEST_FIT:

				// compare by available time, if the difference is relevant
				if((new_server->state == SS_BOOTING && (new_avail - job.submit_time) >= job.est_runtime) || (cur_server->state == SS_BOOTING && (cur_avail - job.submit_time) >= job.est_runtime)) {
					if(new_avail < cur_avail) break;
					else if(new_avail > cur_avail) continue;
				}

				// compare by best-fit
				if(new_margin.cores < cur_margin.cores || new_margin < cur_margin) break;
				else if(new_margin.cores > cur_margin.cores || new_margin > cur_margin) continue;

				// compare by cost
				if(new_server->type->rate < cur_server->type->rate) break;
				continue;

			case SM_START_NEW:

				// start large servers but don't take the last one
				if(all_resc_larger(new_margin, cur_server->type->max_resc) && inactive_of_type(config, new_server->type) > 2) break;
				else if(all_resc_larger(cur_margin, new_server->type->max_resc) && inactive_of_type(config, cur_server->type) > 2) continue;
					
				// account for boot time if relevant
				if(new_server->type->bootTime <= job.est_runtime && cur_server->type->bootTime <= job.est_runtime) {
					// best-fit if boot time isn't relevant
					if(new_margin.cores < cur_margin.cores || new_margin < cur_margin) break;
					else if(new_margin.cores > cur_margin.cores || new_margin > cur_margin) continue;
				}

				// if possible, take the one with lower bootup time
				if(new_server->type->bootTime < cur_server->type->bootTime) break;
				else if(new_server->type->bootTime > cur_server->type->bootTime) continue;

				// compare by cost
				if(new_server->type->rate < cur_server->type->rate) break;
				continue;

			case SM_PREDICTIVE:

				// compare by weighted available time
				if(new_avail + (1 + new_delayed) * job.est_runtime <= cur_avail) break;
				else if(new_avail >= cur_avail + (1 + cur_delayed) * job.est_runtime) continue;

				// compare by available time
				if(new_avail < cur_avail) break;
				else if(new_avail > cur_avail) continue;

				// compare by potentially delayed jobs
				if(new_delayed < cur_delayed) break;
				else if(new_delayed > cur_delayed) continue;
				
				// step forward in time and perform best-fit as usual
				if((new_delayed == 0 && cur_delayed == 0) || (has_zeroed_resc(new_margin) && has_zeroed_resc(cur_margin))) {
					if(new_margin.cores < cur_margin.cores || new_margin < cur_margin) break;
					else if(new_margin.cores > cur_margin.cores || new_margin > cur_margin) continue;

				} else { // try to leave resources to run the delayed jobs
					if(new_margin.cores > cur_margin.cores || new_margin > cur_margin) break;
					else if(new_margin.cores < cur_margin.cores || new_margin < cur_margin) continue;
				}

				// compare by cost
				if(new_server->type->rate < cur_server->type->rate)break;
				continue;

		}
		
		// update current selection
		cur_server = new_server;
		cur_margin = new_margin;
		cur_avail = new_avail;
		cur_mode = new_mode;
		cur_delayed = new_delayed;
	}

	return cur_server;
}