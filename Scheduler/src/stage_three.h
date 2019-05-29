#pragma once
#ifndef stage_three_h_
#define stage_three_h_

#ifdef __cplusplus
#include "cpp_util.h"
#ifndef EXTERN_C
#define EXTERN_C
#define EXTERN_C_stage_three_h_
extern "C" {
#endif
#else
#define noexcept
#include <stdbool.h>
#endif

#include "algorithms.h"

server_info *stage_three(system_config* config, job_info job);

#ifdef __cplusplus
#ifdef EXTERN_C_stage_three_h_
}
#undef EXTERN_C_stage_three_h_
#undef EXTERN_C
#endif
#else
#undef noexcept
#endif

#endif
