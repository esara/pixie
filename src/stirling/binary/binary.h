#pragma once

#include <absl/base/internal/spinlock.h>
#include <absl/container/flat_hash_map.h>

#include "src/common/base/base.h"
#include "src/common/event/event.h"
#include "src/stirling/stirling.h"

namespace binary {

class Binary {
 public:
  Binary();

  void Run();

 private:
  px::Status DataProcessorCallBack(
      uint64_t table_id, px::types::TabletID /* tablet_id */,
      std::unique_ptr<px::types::ColumnWrapperRecordBatch> record_batch);
  void InitDataParsers();

  std::unique_ptr<px::stirling::Stirling> stirling_;
  absl::flat_hash_map<uint64_t, px::stirling::stirlingpb::InfoClass>
      table_info_map_;
  absl::base_internal::SpinLock callback_state_lock_;
  // Member variables needed to initialize the dispatcher.
  std::unique_ptr<px::event::TimeSystem> time_system_;
  px::event::APIUPtr api_;
  px::event::DispatcherUPtr dispatcher_;
};

}  // namespace kfuse
