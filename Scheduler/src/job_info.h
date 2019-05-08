#pragma once
#ifndef job_info_h_
#define job_info_h_

#include "resource_info.h"

#ifdef __cplusplus
extern "C" {
#else
#define noexcept
#include <stdbool.h>
#endif

typedef struct job_info {
	unsigned submit_time; // time the job was submitted to the client
	unsigned id; // unique identifier for the job
	unsigned est_runtime; // esimated runtime of the job (milliseconds)
	resource_info req_resc; // resources required to run the job
#ifdef __cplusplus
	// whether a job can run on given resources
	bool can_run(const resource_info &resc) const noexcept;
	// the fitness for a job on given resources
	int fitness(const resource_info &resc) const noexcept;
#endif
} job_info;

/* create job struct from string
 * format of string is:
 * JOBN {SUBMIT_TIME} {ID} {RUN_TIME} {CORES} {MEMORY} {DISK}
 */
job_info job_from_string(const char *jobstr) noexcept;

// whether a job can run on given resources, wraps job_info.can_run
bool job_can_run(const job_info *job, const resource_info resc) noexcept;

// the fitness for a job on given resources, wraps job_info.fitness
int job_fitness(const job_info *job, const resource_info resc) noexcept;

#ifdef __cplusplus
}
#else
#undef noexcept
#endif

#endif
