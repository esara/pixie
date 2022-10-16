#include "src/stirling/binary/binary.h"
#include "src/stirling/core/output.h"

namespace binary {

Binary::Binary()
  : stirling_(px::stirling::Stirling::Create(px::stirling::CreateProdSourceRegistry())),
    time_system_(std::make_unique<px::event::RealTimeSystem>()),
    api_(std::make_unique<px::event::APIImpl>(time_system_.get())),
    dispatcher_(api_->AllocateDispatcher("binary")) {
  px::stirling::stirlingpb::Publish publication;
  stirling_->GetPublishProto(&publication);
  px::stirling::IndexPublication(publication, &table_info_map_);
  stirling_->RegisterDataPushCallback(
      std::bind(&Binary::DataProcessorCallBack, this,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3));
}

void Binary::Run() {
  PL_EXIT_IF_ERROR(stirling_->RunAsThread());
  dispatcher_->Run(px::event::Dispatcher::RunType::Block);
}

px::Status Binary::DataProcessorCallBack(
    uint64_t table_id, px::types::TabletID /* tablet_id */,
    std::unique_ptr<px::types::ColumnWrapperRecordBatch> record_batch) {
  absl::base_internal::SpinLockHolder lock(&callback_state_lock_);

  // Find the table info from the publications.
  auto iter = table_info_map_.find(table_id);
  if (iter == table_info_map_.end()) {
    return px::error::Internal("Encountered unknown table id $0", table_id);
  }
  const px::stirling::stirlingpb::InfoClass& table_info = iter->second;

  LOG(INFO) << px::stirling::ToString(table_info.schema().name(),
                                      table_info.schema(), *record_batch);
  return px::Status::OK();
}

}  // namespace binary
