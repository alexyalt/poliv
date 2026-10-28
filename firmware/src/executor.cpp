#include "executor.h"
#include <algorithm>

namespace poliv {
uint16_t relayBits(uint8_t zone, bool activeHigh) {
  uint16_t bits = zone >= 1 && zone <= 16 ? uint16_t(1u << (zone - 1)) : 0;
  return activeHigh ? bits : uint16_t(~bits);
}
Executor::Executor(IRelayDriver& r, IJournal& j, uint32_t maxSeconds)
    : relays_(r), journal_(j), maxSeconds_(maxSeconds) { relays_.select(0); }
bool Executor::marked(const std::string& id) const {
  return std::find(done_.begin(), done_.end(), id) != done_.end();
}
bool Executor::apply(const Schedule& s, bool persist) {
  if (!s.version || s.hash.empty() || s.runs.size() > 128 || s.maxDelay > 86400) return false;
  if (s.version < schedule_.version) return false;
  if (s.version == schedule_.version) return s.hash == schedule_.hash;
  std::vector<std::string> ids;
  size_t count = 0;
  for (const auto& r : s.runs) {
    if (r.id.empty() || r.id.size() > 96 || r.start <= 0 || r.steps.empty() ||
        std::find(ids.begin(), ids.end(), r.id) != ids.end()) return false;
    ids.push_back(r.id);
    for (const auto& st : r.steps) {
      if (++count > 1024 || st.zone > 16 || !st.seconds || st.seconds > maxSeconds_ ||
          (st.phase != "water" && st.phase != "gap" && st.phase != "soak") ||
          ((st.phase == "water") != (st.zone != 0))) return false;
    }
  }
  // Transport applies a replacement only after finishing/cancelling a current run.
  if (cursor_.active || suspended_.active) return false;
  schedule_ = s;
  done_.clear();
  return !persist || checkpoint();
}
bool Executor::checkpoint() {
  if (journal_.save(manual_ ? suspended_ : cursor_, done_)) return true;
  relays_.select(0); zone_ = 0; error_ = "storage_error"; phase_ = "idle";
  return false;
}
bool Executor::record(const std::string& status) {
  if (!zone_) return true;
  if (journal_.event(cursor_.run, zone_, status, manual_ ? "manual" : "schedule", elapsed_)) return true;
  relays_.select(0); zone_ = 0; error_ = "event_buffer_full"; phase_ = "idle";
  return false;
}
void Executor::restore(const Cursor& c, const std::vector<std::string>& done, bool resumeRun) {
  relays_.select(0); zone_ = 0; done_ = done;
  if (!c.active) return;
  const auto it = std::find_if(schedule_.runs.begin(), schedule_.runs.end(),
                             [&](const Run& r){return r.id == c.run;});
  if (resumeRun && it != schedule_.runs.end() && c.step < it->steps.size() &&
      c.remaining && c.remaining <= it->steps[c.step].seconds) {
    cursor_ = c; // open only after valid wall-clock time in tick().
  } else {
    if (!marked(c.run)) done_.push_back(c.run);
    cursor_ = {};
  }
  checkpoint();
}
bool Executor::openStep() {
  auto r = std::find_if(schedule_.runs.begin(), schedule_.runs.end(),
                        [&](const Run& r){return r.id == cursor_.run;});
  if (r == schedule_.runs.end() || cursor_.step >= r->steps.size()) return false;
  const auto& st = r->steps[cursor_.step];
  if (!cursor_.remaining) cursor_.remaining = st.seconds;
  elapsed_ = 0;
  if (!checkpoint()) return false;
  phase_ = st.phase; zone_ = st.zone;
  if (!record("started")) return false;
  relays_.setTimeout(cursor_.remaining);
  relays_.select(zone_);
  return true;
}
void Executor::finish(bool aborted) {
  // De-energize before any flash/network operation.
  relays_.select(0);
  bool ok = record(aborted ? "aborted" : "done");
  zone_ = 0; phase_ = "idle";
  if (manual_) {
    manual_ = false; cursor_ = suspended_; suspended_ = {};
  } else {
    if (!marked(cursor_.run)) done_.push_back(cursor_.run);
    cursor_ = {};
  }
  if (ok) checkpoint();
}
void Executor::tick(int64_t unixSeconds, uint32_t mono, bool timeValid) {
  uint32_t delta = clockStarted_ ? mono - lastMono_ : 0;
  clockStarted_ = true; lastMono_ = mono;
  if (!error_.empty() || paused_) return;
  if (cursor_.active) {
    if (phase_ == "idle") {
      if (!timeValid && !manual_) return;
      if (!manual_ && !openStep()) fault("invalid_cursor");
      return;
    }
    if (delta >= cursor_.remaining) {
      elapsed_ += cursor_.remaining;
      relays_.select(0);
      if (manual_) { finish(false); return; }
      if (!record("done")) return;
      zone_ = 0; phase_ = "idle";
      auto r = std::find_if(schedule_.runs.begin(), schedule_.runs.end(),
                           [&](const Run& r){return r.id == cursor_.run;});
      ++cursor_.step; cursor_.remaining = 0;
      if (r == schedule_.runs.end() || cursor_.step >= r->steps.size()) {
        if (!marked(cursor_.run)) done_.push_back(cursor_.run);
        cursor_ = {}; checkpoint();
      } else { openStep(); }
    } else {
      cursor_.remaining -= delta; elapsed_ += delta;
      if (delta && elapsed_ / 5 != (elapsed_ - delta) / 5) checkpoint();
    }
    return;
  }
  if (!timeValid) return;
  const Run* best = nullptr;
  for (const auto& r : schedule_.runs) {
    if (marked(r.id) || r.start > unixSeconds) continue;
    if (unixSeconds - r.start > schedule_.maxDelay) {
      done_.push_back(r.id); if (!checkpoint()) return;
    } else if (!best || r.start < best->start) best = &r;
  }
  if (best) { cursor_ = {best->id, 0, 0, true}; openStep(); }
}
bool Executor::manual(uint8_t zone, uint32_t seconds, uint32_t mono) {
  if (!zone || zone > 16 || !seconds || seconds > maxSeconds_ || !error_.empty() || paused_) return false;
  if (manual_) finish(true);
  if (!error_.empty()) return false;
  relays_.select(0);
  if (zone_ && !record("done")) return false;
  zone_ = 0;
  suspended_ = cursor_; manual_ = true;
  cursor_ = {"manual-" + std::to_string(mono), 0, seconds, true};
  if (!checkpoint()) return false;
  lastMono_ = mono; clockStarted_ = true; elapsed_ = 0;
  zone_ = zone; phase_ = "water";
  if (!record("started")) return false;
  relays_.setTimeout(cursor_.remaining);
  relays_.select(zone_); return true;
}
void Executor::stop() {
  relays_.select(0);
  if (manual_) finish(true);
  if (cursor_.active) finish(true);
  zone_ = 0; phase_ = "idle";
}
void Executor::pause() {
  relays_.select(0);
  // Finish a manual run; preserve the schedule's remaining step.
  if (manual_) finish(true);
  if (zone_) record("done");
  zone_ = 0; phase_ = "idle"; paused_ = true; checkpoint();
}
void Executor::resume() { paused_ = false; }
void Executor::fault(const std::string& code) {
  stop(); error_ = code; relays_.select(0); zone_ = 0;
}
bool Executor::clearFault() {
  if (error_ == "storage_error" || error_ == "event_buffer_full") return false;
  error_.clear(); return true;
}
std::string Executor::mode() const {
  if (!error_.empty()) return "error";
  if (paused_) return "paused";
  return manual_ ? "manual" : cursor_.active ? "schedule" : "idle";
}
}
