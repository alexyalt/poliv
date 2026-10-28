#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace poliv {
struct IRelayDriver {
  virtual ~IRelayDriver() = default;
  // 0 closes everything; 1..16 selects exactly one zone.
  virtual void select(uint8_t zone) = 0;
  virtual void setTimeout(uint32_t) {}
};
struct Step { uint8_t zone = 0; uint32_t seconds = 0; std::string phase = "water"; };
struct Run { std::string id; int64_t start = 0; std::vector<Step> steps; };
struct Schedule { uint32_t version = 0; std::string hash; uint32_t maxDelay = 120; std::vector<Run> runs; };
struct Cursor {
  std::string run;
  uint32_t step = 0, remaining = 0;
  bool active = false;
};
struct IJournal {
  virtual ~IJournal() = default;
  // Must durably commit before opening a valve; false must leave valves closed.
  virtual bool save(const Cursor& cursor, const std::vector<std::string>& done) = 0;
  virtual bool event(const std::string& run, uint8_t zone, const std::string& status,
                     const std::string& source, uint32_t seconds) = 0;
};
class Executor {
 public:
  Executor(IRelayDriver& relays, IJournal& journal, uint32_t maxSeconds = 3600);
  bool apply(const Schedule& schedule, bool persist = true);
  void restore(const Cursor& cursor, const std::vector<std::string>& done, bool resume);
  void tick(int64_t unixSeconds, uint32_t monotonicSeconds, bool timeValid);
  bool manual(uint8_t zone, uint32_t seconds, uint32_t mono);
  void stop();
  void pause();
  void resume();
  void fault(const std::string& code);
  bool clearFault();
  uint8_t zone() const { return zone_; }
  const std::string& error() const { return error_; }
  const std::string& phase() const { return phase_; }
  std::string mode() const;
  const Cursor& cursor() const { return cursor_; }
  const Schedule& schedule() const { return schedule_; }
 private:
  bool checkpoint();
  bool record(const std::string& status);
  bool openStep();
  void finish(bool aborted);
  bool marked(const std::string& id) const;
  IRelayDriver& relays_;
  IJournal& journal_;
  uint32_t maxSeconds_, lastMono_ = 0, elapsed_ = 0;
  bool clockStarted_ = false, paused_ = false, manual_ = false;
  uint8_t zone_ = 0;
  std::string error_, phase_ = "idle";
  Schedule schedule_;
  Cursor cursor_, suspended_;
  std::vector<std::string> done_;
};
uint16_t relayBits(uint8_t zone, bool activeHigh);
}
