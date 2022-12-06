/*
 * Copyright 2018- The Pixie Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include <csignal>
#include <iostream>
#include <thread>

#include <absl/base/internal/spinlock.h>
#include <absl/strings/str_split.h>
#include <google/protobuf/text_format.h>

#include "src/common/base/base.h"
#include "src/common/base/utils.h"
#include "src/common/perf/profiler.h"
#include "src/common/signal/signal.h"
#include "src/stirling/core/output.h"
#include "src/stirling/core/pub_sub_manager.h"
#include "src/stirling/core/source_registry.h"
#include "src/stirling/source_connectors/dynamic_tracer/dynamic_tracing/dynamic_tracer.h"
#include "src/stirling/stirling.h"
#include "src/stirling/HTTPRequest.h"

#ifdef PXL_SUPPORT
#include "src/carnot/planner/probes/tracepoint_generator.h"
#include "src/shared/tracepoint_translation/translation.h"
#endif

using ::px::ProcessStatsMonitor;

using ::px::Status;
using ::px::StatusOr;

using ::px::stirling::CreateSourceRegistryFromFlag;
using ::px::stirling::IndexPublication;
using ::px::stirling::SourceConnectorGroup;
using ::px::stirling::SourceRegistry;
using ::px::stirling::Stirling;
using ::px::stirling::ToString;
using ::px::stirling::stirlingpb::InfoClass;
using ::px::stirling::stirlingpb::Publish;
using ::px::types::ColumnWrapperRecordBatch;
using ::px::types::TabletID;

using DynamicTracepointDeployment =
    ::px::stirling::dynamic_tracing::ir::logical::TracepointDeployment;

DEFINE_int32(timeout_secs, -1,
             "If non-negative, only runs for the specified amount of time and exits.");
DEFINE_bool(color_output, true, "If true, output logs will use colors.");

// Put this in global space, so we can kill it in the signal handler.
Stirling* g_stirling = nullptr;
ProcessStatsMonitor* g_process_stats_monitor = nullptr;

//-----------------------------------------------------------------------------
// Callback/Printing Code
//-----------------------------------------------------------------------------

absl::flat_hash_map<uint64_t, InfoClass> g_table_info_map;
absl::base_internal::SpinLock g_callback_state_lock;

void send(const std::string& body) {
  try
  {
      http::Request request{"http://localhost:8080"};
      const auto response = request.send("POST", body, {
          {"Content-Type", "application/json"}
      });

      if (response.status.code != 200) {
        std::cout << response.status.code << std::endl;
      }
  }
  catch (const std::exception& e)
  {
      std::cerr << "Request failed, error: " << e.what() << '\n';
  }
}

Status StirlingWrapperCallback(uint64_t table_id, TabletID /* tablet_id */,
                               std::unique_ptr<ColumnWrapperRecordBatch> record_batch) {
  absl::base_internal::SpinLockHolder lock(&g_callback_state_lock);

  // Find the table info from the publications.
  auto iter = g_table_info_map.find(table_id);
  if (iter == g_table_info_map.end()) {
    return px::error::Internal("Encountered unknown table id $0", table_id);
  }
  const InfoClass& table_info = iter->second;

  // Only output enabled tables (lookup by name).
  const std::string& json = ToString(table_info.schema().name(), table_info.schema(), *record_batch);
  send(json);

  return Status::OK();
}

//-----------------------------------------------------------------------------
// Signal Handling Code
//-----------------------------------------------------------------------------

// This signal handler is meant for graceful termination.
void TerminationHandler(int signum) {
  std::cerr << "\n\nStopping, might take a few seconds ..." << std::endl;
  // Important to call Stop(), because it releases BPF resources,
  // which would otherwise leak.
  if (g_stirling != nullptr) {
    g_stirling->Stop();
  }

  if (g_process_stats_monitor != nullptr) {
    g_process_stats_monitor->PrintCPUTime();
  }
  exit(signum);
}

// DeathHandler is meant for fatal errors (like seg-faults),
// where no graceful termination is performed.
class DeathHandler : public px::FatalErrorHandlerInterface {
 public:
  DeathHandler() = default;
  void OnFatalError() const override {}
};

std::unique_ptr<px::SignalAction> g_signal_action;

//-----------------------------------------------------------------------------
// Main
//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
  // Register signal handlers to gracefully clean-up on exit.
  signal(SIGINT, TerminationHandler);
  signal(SIGQUIT, TerminationHandler);
  signal(SIGTERM, TerminationHandler);
  signal(SIGHUP, TerminationHandler);

  // This handles fatal (non-graceful) errors.
  g_signal_action = std::make_unique<px::SignalAction>();
  DeathHandler err_handler;
  g_signal_action->RegisterFatalErrorHandler(err_handler);

  px::EnvironmentGuard env_guard(&argc, argv);
  // Override the default coloring set by the environment. This is useful for tests.
  FLAGS_colorlogtostderr = FLAGS_color_output;

  LOG(INFO) << "Stirling Wrapper PID: " << getpid();

  // Make Stirling.
  std::unique_ptr<Stirling> stirling = Stirling::Create(CreateSourceRegistryFromFlag());
  g_stirling = stirling.get();

  // Enable use of USR1/USR2 for controlling debug.
  stirling->RegisterUserDebugSignalHandlers();

  // Get a publish proto message to subscribe from.
  Publish publication;
  stirling->GetPublishProto(&publication);
  IndexPublication(publication, &g_table_info_map);

  // Set a callback function that outputs the pushed records.
  stirling->RegisterDataPushCallback(StirlingWrapperCallback);

  // Start measuring process stats after init.
  ProcessStatsMonitor process_stats_monitor;
  g_process_stats_monitor = &process_stats_monitor;

  // Run Data Collector.
  std::thread run_thread = std::thread(&Stirling::Run, stirling.get());

  // Wait until thread starts.
  PL_CHECK_OK(stirling->WaitUntilRunning(/* timeout */ std::chrono::seconds(5)));

  if (FLAGS_timeout_secs >= 0) {
    // Run for the specified amount of time, then terminate.
    LOG(INFO) << absl::Substitute("Running for $0 seconds.", FLAGS_timeout_secs);
    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_timeout_secs));
    stirling->Stop();
  }

  // Wait for the thread to return.
  // This should never happen unless --timeout_secs is specified.
  run_thread.join();

  // Another model of how to run Stirling:
  // stirling->RunAsThread();
  // stirling->WaitForThreadJoin();

  return 0;
}
