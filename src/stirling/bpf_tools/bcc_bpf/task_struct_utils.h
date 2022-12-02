/*
 * This code runs using bpf in the Linux kernel.
 * Copyright 2018- The Pixie Authors.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 *
 * SPDX-License-Identifier: GPL-2.0
 */

// LINT_C_FILE: Do not remove this line. It ensures cpplint treats this as a C file.

#pragma once

#include <linux/param.h>

// TODO: move
#define NSEC_PER_SEC 1000000000L
#define USER_HZ 100

#include "src/stirling/bpf_tools/bcc_bpf/utils.h"

// This is how Linux converts nanoseconds to clock ticks.
// Used to report PID start times in clock ticks, just like /proc/<pid>/stat does.
static __inline uint64_t pl_nsec_to_clock_t(uint64_t x) {
  return x / (NSEC_PER_SEC / USER_HZ);
  // return div_u64(x, NSEC_PER_SEC / USER_HZ);
}

static __inline uint64_t read_start_boottime(const struct task_struct* task) {
  struct task_struct* group_leader_ptr = 0;
  BPF_CORE_READ_INTO(&group_leader_ptr, task, group_leader);

  uint64_t start_boottime = 0;
  BPF_CORE_READ_INTO(&start_boottime, group_leader_ptr, real_start_time);

  return pl_nsec_to_clock_t(start_boottime);
}

static __inline uint64_t read_start_boottime2(const struct task_struct___2* task) {
  struct task_struct* group_leader_ptr = 0;
  BPF_CORE_READ_INTO(&group_leader_ptr, task, group_leader);

  uint64_t start_boottime = 0;
  BPF_CORE_READ_INTO(&start_boottime, group_leader_ptr, real_start_time);

  return pl_nsec_to_clock_t(start_boottime);
}

static __inline uint64_t get_tgid_start_time() {
  struct task_struct* task = (struct task_struct*)bpf_get_current_task();
  return read_start_boottime(task);
}