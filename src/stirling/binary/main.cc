#include "src/common/signal/signal.h"
#include "src/stirling/binary/binary.h"

class FatalErrorHandler : public px::FatalErrorHandlerInterface {
 public:
  FatalErrorHandler() = default;
  void OnFatalError() const override {
    // Stack trace will print automatically; any additional state dumps can be done here.
    // Note that actions here must be async-signal-safe and must not allocate memory.
  }
};

int main(int argc, char** argv) {
  px::EnvironmentGuard env_guard(&argc, argv);

  FatalErrorHandler err_handler;
  // This covers signals such as SIGSEGV and other fatal errors.
  // We print the stack trace and die.
  auto signal_action = std::make_unique<px::SignalAction>();
  signal_action->RegisterFatalErrorHandler(err_handler);

  LOG(INFO) << "Starting Binary";
  binary::Binary binary;
  binary.Run();
}
