#include "stage_three.h"

#include <map>
#include <set>
#include <algorithm>
#include <limits>
#include <cstdint>

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


// general idea: schedule to the server that would have the least net change in resource avaiability: closest to full or closest to empty
// always prefer currently running servers
server_info *stage_three(system_config* config, job_info job) {
	server_info *bf_avail_server, *wf_avail_server, *bf_wait_server, *wf_wait_server;
	bf_avail_server = wf_avail_server = bf_wait_server = wf_wait_server = nullptr;
	resource_info bf_avail_margin, wf_avail_margin, bf_wait_margin, wf_wait_margin;
	bf_avail_margin = bf_wait_margin = RESC_MAX;
	wf_avail_margin = wf_wait_margin = RESC_MIN;
	intmax_t bf_wait_time, wf_wait_time;
	bf_wait_time = wf_wait_time = std::numeric_limits<intmax_t>::max();

	// maybe also look at parsing lstj?

	for(auto s = 0; s < config->num_servers; ++s) {
		auto *server = &config->servers[s];
		auto new_margin = resc_diff(job.req_resc, server->avail_resc);
		if(server->avail_time <= static_cast<intmax_t>(job.submit_time)) {
			// look at avail servers
		} else {
			// look at servers we'd have to wait for
		}
	}
}