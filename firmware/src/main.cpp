#include <Arduino.h>
#include <ArduinoJson.h>
#include <WiFi.h>
#include <MQTT.h>
#include <LittleFS.h>
#include <Preferences.h>
#include <esp_task_wdt.h>
#include <esp_timer.h>
#include <sys/time.h>
#include <algorithm>
#include <deque>
#include "executor.h"
#include "settings.h"
using namespace poliv;
constexpr size_t MaxPayload = 48 * 1024;
constexpr size_t MaxEvents = 128;
String box, prefix;
uint32_t mono() { return uint32_t(esp_timer_get_time() / 1000000); }
bool validTime() { return time(nullptr) > 1735689600; }
class Relays : public IRelayDriver {
 public:
  void begin() { pinMode(13,OUTPUT);pinMode(12,OUTPUT);pinMode(14,OUTPUT);select(0); }
  void select(uint8_t zone) override {
    uint16_t bits=relayBits(OUTPUTS_VERIFIED ? zone : 0, RELAY_ACTIVE_HIGH);
    digitalWrite(14,LOW);shiftOut(13,12,MSBFIRST,bits >> 8);
    shiftOut(13,12,MSBFIRST,bits & 255);digitalWrite(14,HIGH);
  }
} relays;
WiFiClient network;
MQTTClient mqtt(MaxPayload + 1024);
Preferences nvs;
String uid() { char b[25];snprintf(b,sizeof(b),"%08lx%08lx%08lx",(unsigned long)esp_random(),(unsigned long)esp_random(),(unsigned long)esp_random());return String(b); }
void base(JsonDocument& d) { d["protocol_version"]="1.0";d["box_id"]=box;d["ts"]=int64_t(time(nullptr)); }
bool publish(const char* kind, JsonDocument& d, bool retained=false) {
 String raw;serializeJson(d,raw);return mqtt.connected() && mqtt.publish(prefix+kind,raw,retained,1);
}
// Atomic rename preserves the last valid record if a write fails. No auto-format on boot.
bool atomicJson(const char* path, JsonDocument& d) {
 String tmp=String(path)+".tmp";File f=LittleFS.open(tmp,"w");if(!f)return false;
 size_t written=serializeJson(d,f);f.flush();f.close();
 if(written!=measureJson(d)){LittleFS.remove(tmp);return false;}
 return LittleFS.rename(tmp,path);
}
bool readJson(const char* path, JsonDocument& d) {
 File f=LittleFS.open(path,"r");if(!f || f.size()>MaxPayload)return false;
 return !deserializeJson(d,f);
}
class Journal : public IJournal {
 public:
  std::deque<String> events;
  bool healthy=false;
  bool save(const Cursor& c,const std::vector<std::string>& done) override {
    if(!healthy)return false;
    JsonDocument d;d["version"]=version;d["run"]=c.run;d["step"]=c.step;
    d["remaining"]=c.remaining;d["active"]=c.active;
    auto a=d["done"].to<JsonArray>();for(const auto& id:done)a.add(id);
    return atomicJson("/cursor.json",d);
  }
  uint32_t version=0;
  bool persistEvents() {
    JsonDocument d;auto a=d["events"].to<JsonArray>();for(const auto& s:events)a.add(s);
    return atomicJson("/events.json",d);
  }
  bool event(const std::string& run,uint8_t zone,const std::string& status,
             const std::string& source,uint32_t seconds) override {
    if(!healthy || events.size()>=MaxEvents)return false;
    if(!zone)return true; // soak/gap are not water events
    JsonDocument d;base(d);d["event_uid"]=uid();d["source"]=source;
    d["status"]=status=="started"?"started":status=="aborted"?"stopped":"finished";
    d["buffered"]=true;d["aborted"]=status=="aborted";
    if(source=="schedule")d["run_id"]=run;
    int64_t end=time(nullptr);d["start_ts"]=end-seconds;d["end_ts"]=end;
    d["water_sec"]=seconds;d["volume_liters"]=nullptr;
    d["active_zones"].to<JsonArray>().add(zone);
    String raw;serializeJson(d,raw);events.push_back(raw);
    if(persistEvents())return true;events.pop_back();return false;
  }
  bool load() {
    healthy=LittleFS.begin(false);if(!healthy)return false;
    if(!LittleFS.exists("/events.json"))return true;
    JsonDocument d;if(!readJson("/events.json",d) || !d["events"].is<JsonArray>())return healthy=false;
    for(auto s:d["events"].as<JsonArray>()) {
      if(!s.is<const char*>() || events.size()>=MaxEvents)return healthy=false;
      events.push_back(s.as<String>());
    }
    return true;
  }
  void drain() {
    // QoS1 publish waits for broker PUBACK; keep original UID across retries/reboot.
    if(events.empty() || !mqtt.connected())return;
    if(mqtt.publish(prefix+"event",events.front(),false,1)) {
      String front=events.front();events.pop_front();
      if(!persistEvents()){events.push_front(front);healthy=false;}
    }
  }
} journal;
Executor executor(relays,journal);
volatile uint32_t flowPulses=0;
portMUX_TYPE flowMux=portMUX_INITIALIZER_UNLOCKED;
void IRAM_ATTR pulse() {portENTER_CRITICAL_ISR(&flowMux);++flowPulses;portEXIT_CRITICAL_ISR(&flowMux);}
uint32_t pulses() {portENTER_CRITICAL(&flowMux);auto n=flowPulses;portEXIT_CRITICAL(&flowMux);return n;}
float lpm=0, liters=0;
// Decode server date + local minute to UTC; reject normalized invalid dates.
bool decode(JsonDocument& d,Schedule& s) {
 if(d["protocol_version"]!="1.0" || d["box_id"]!=box ||
    !d["schedule_version"].is<uint32_t>() || !d["runs"].is<JsonArray>())return false;
 s.version=d["schedule_version"];s.hash=d["schedule_hash"] | "";
 s.maxDelay=d["options"]["max_start_delay_sec"] | 120u;
 int offset=d["timezone_offset_min"] | 0;if(offset < -840 || offset > 840)return false;
 if(d["runs"].size()>128)return false;
 size_t total=0;
 for(JsonObject r:d["runs"].as<JsonArray>()) {
  if(r["enabled"].is<bool>() && !r["enabled"].as<bool>())continue;
  Run run;run.id=r["run_id"] | "";
  const char* date=r["date"] | "";int y=0,m=0,day=0;
  if(strlen(date)!=10 || sscanf(date,"%4d-%2d-%2d",&y,&m,&day)!=3 || y<2025 || y>2099)return false;
  int minute=r["start_minute_local"] | -1;if(minute<0 || minute>=1440)return false;
  tm t={};t.tm_year=y-1900;t.tm_mon=m-1;t.tm_mday=day;
  int64_t midnight=mktime(&t);
  if(t.tm_year!=y-1900 || t.tm_mon!=m-1 || t.tm_mday!=day)return false;
  run.start=midnight+minute*60-offset*60;
  if(!r["steps"].is<JsonArray>())return false;
  for(JsonObject st:r["steps"].as<JsonArray>()) {
   if(++total>1024 || !st["duration_sec"].is<uint32_t>() || !st["active_zones"].is<JsonArray>())return false;
   auto zones=st["active_zones"].as<JsonArray>();if(zones.size()>1)return false;
   uint8_t zone=0;if(zones.size()) {
    if(!zones[0].is<int>() || zones[0].as<int>()<1 || zones[0].as<int>()>16)return false;
    zone=zones[0].as<uint8_t>();
   }
   run.steps.push_back({zone,st["duration_sec"].as<uint32_t>(),st["phase"] | "water"});
  }
  s.runs.push_back(std::move(run));
 }
 // Validate without touching running outputs or durable state.
 struct NullRelay:IRelayDriver{void select(uint8_t) override{}} r;
 struct NullJournal:IJournal{
  bool save(const Cursor&,const std::vector<std::string>&) override{return true;}
  bool event(const std::string&,uint8_t,const std::string&,const std::string&,uint32_t) override{return true;}
 } j;
 Executor candidate(r,j);return candidate.apply(s);
}
void status() {
 JsonDocument d;base(d);d["online"]=true;d["mode"]=!OUTPUTS_VERIFIED ? "service" : executor.mode();d["phase"]=executor.phase()=="gap" ? "waiting" : executor.phase();
 auto a=d["active_zones"].to<JsonArray>();auto b=d["display_zones"].to<JsonArray>();
 if(executor.zone()){a.add(executor.zone());b.add(executor.zone());d["primary_zone"]=executor.zone();}
 else d["primary_zone"]=nullptr;
 d["schedule_version"]=executor.schedule().version;d["schedule_hash"]=executor.schedule().hash;
 d["time_valid"]=validTime();d["buffer_size"]=journal.events.size();d["flow_enabled"]=FLOW_ENABLED;
 d["flow_total_liters"]=liters;d["instant_lpm"]=lpm;d["service_mode"]=!OUTPUTS_VERIFIED;
 if(executor.error().empty())d["error_code"]=nullptr;else d["error_code"]=executor.error();
 publish("status",d);
}
std::deque<String> commands;
bool remember(const String& id) {
 if(id.isEmpty() || id.length()>96)return false;
 commands.push_back(id);if(commands.size()>50)commands.pop_front();
 JsonDocument d;auto a=d["ids"].to<JsonArray>();for(auto& s:commands)a.add(s);
 return atomicJson("/commands.json",d);
}
// Inbound callback only queues data: QoS publish in a callback can deadlock MQTTClient.
struct Incoming {String topic,raw;};
std::deque<Incoming> incoming;
void receive(String& topic,String& raw) {
 if(raw.length()>MaxPayload)return;
 if(incoming.size()>=4){executor.fault("inbound_overflow");return;}
 if(topic.endsWith("/command") && raw.indexOf("stop_all")>=0)incoming.push_front({topic,raw});
 else incoming.push_back({topic,raw});
}
void process(Incoming message) {
 JsonDocument d;if(deserializeJson(d,message.raw))return;
 if(d["protocol_version"]!="1.0" || d["box_id"]!=box)return;
 if(message.topic==prefix+"time") {
  int64_t ts=d["unix_ts"] | int64_t(0);if(ts>1735689600){timeval tv={time_t(ts),0};settimeofday(&tv,nullptr);}return;
 }
 if(message.topic==prefix+"schedule") {
  Schedule s;bool ok=decode(d,s);
  if(ok && s.version==executor.schedule().version)ok=s.hash==executor.schedule().hash;
  else if(ok && s.version>executor.schedule().version && !executor.cursor().active && executor.mode()=="idle") {
   // Schedule is durable first. Version in cursor prevents using an old cursor after a power cut.
   ok=atomicJson("/schedule.json",d);
   if(ok){journal.version=s.version;ok=executor.apply(s);}
  } else ok=false;
  JsonDocument a;base(a);a["schedule_version"]=d["schedule_version"];a["schedule_hash"]=d["schedule_hash"];
  a["status"]=ok?"applied":"rejected";a["message"]=ok?"Расписание сохранено":"Недопустимое расписание, старая версия или контроллер занят";
  publish("schedule_ack",a);status();return;
 }
 if(message.topic!=prefix+"command")return;
 String id=d["command_id"] | "";String cmd=d["command"] | "";
 bool duplicate=std::find(commands.begin(),commands.end(),id)!=commands.end();
 bool ok=false;String result="rejected";bool reboot=false;
 // Stop always closes outputs even if the journal is unavailable.
 if(cmd=="stop_all")executor.stop();
 if(duplicate)result="ignored_duplicate";
 else if(journal.healthy && remember(id)) {
  auto p=d["params"].as<JsonObject>();
  if(cmd=="ping")ok=true;
  else if(cmd=="stop_all")ok=true;
  else if(cmd=="zone_close") {if(p["zone"].as<int>()==executor.zone())executor.stop();ok=true;}
  else if(cmd=="zone_open" && OUTPUTS_VERIFIED && p["zone"].is<int>() && p["duration_sec"].is<uint32_t>()) {
   int z=p["zone"];if(z>=1 && z<=16)ok=executor.manual(z,p["duration_sec"],mono());
  }
  else if(cmd=="pause_controller"){executor.pause();ok=true;}
  else if(cmd=="resume_controller"){executor.resume();ok=true;}
  else if(cmd=="clear_error")ok=executor.clearFault();
  else if(cmd=="reboot"){executor.stop();ok=true;reboot=true;}
  if(ok)result="completed";
 }
 JsonDocument a;base(a);a["command_id"]=id;a["command"]=cmd;a["status"]=result;
 a["message"]=ok?"Команда выполнена":"Команда отклонена или уже обработана";publish("command_ack",a);status();
 if(reboot)ESP.restart();
}
void connectMqtt() {
 String will="{\"protocol_version\":\"1.0\",\"box_id\":\""+box+"\",\"online\":false}";
 mqtt.setWill((prefix+"lwt").c_str(),will.c_str(),true,1);
 if(!mqtt.connect(("poliv-"+box).c_str(),MQTT_USER,MQTT_PASSWORD))return;
 mqtt.subscribe(prefix+"command",1);mqtt.subscribe(prefix+"schedule",1);mqtt.subscribe(prefix+"time",1);
 JsonDocument d;base(d);d["online"]=true;publish("lwt",d,true);
 d.clear();base(d);d["firmware_version"]="0.10.0";d["hardware_model"]="ESP32-DevKit-74HC595";
 d["zones_total"]=16;d["ip_address"]=WiFi.localIP().toString();d["mac"]=WiFi.macAddress();
 d["schedule_version"]=executor.schedule().version;d["schedule_hash"]=executor.schedule().hash;
 d["flow_enabled"]=FLOW_ENABLED;d["time_valid"]=validTime();publish("hello",d);
 d.clear();base(d);d["reason"]="startup";d["schedule_version"]=executor.schedule().version;
 d["schedule_hash"]=executor.schedule().hash;publish("schedule_request",d);status();
}
void setup() {
 relays.begin();Serial.begin(115200);
 setenv("TZ","UTC0",1);tzset();
 uint64_t mac=ESP.getEfuseMac();char id[7];snprintf(id,sizeof(id),"%02X%02X%02X",uint8_t(mac>>24),uint8_t(mac>>32),uint8_t(mac>>40));
 box=id;prefix="poliv/"+box+"/";
 if(!journal.load())executor.fault("storage_error");
 if(journal.healthy) {
  JsonDocument d;
  if(LittleFS.exists("/schedule.json")) {
   Schedule s;
   if(!readJson("/schedule.json",d) || !decode(d,s))executor.fault("invalid_schedule");
   else {
    // Read cursor before apply() creates an initial checkpoint.
    JsonDocument c;bool exists=LittleFS.exists("/cursor.json");bool valid=exists && readJson("/cursor.json",c);
    journal.version=s.version;
    if(executor.apply(s)) {
     if(exists && !valid)executor.fault("invalid_cursor");
     else if(valid && c["version"].as<uint32_t>()==s.version) {
      Cursor cursor={c["run"] | "",c["step"] | 0u,c["remaining"] | 0u,c["active"] | false};
      std::vector<std::string> done;for(auto v:c["done"].as<JsonArray>())done.push_back(v.as<std::string>());
      executor.restore(cursor,done,RESUME_AFTER_POWER_LOSS);
     }
    }
   }
  }
  d.clear();if(LittleFS.exists("/commands.json")) {
   if(!readJson("/commands.json",d) || !d["ids"].is<JsonArray>() || d["ids"].size()>50)executor.fault("storage_error");
   else for(auto v:d["ids"].as<JsonArray>())commands.push_back(v.as<String>());
  }
 }
 if(FLOW_ENABLED) {
  if(FLOW_PULSES_PER_LITER<=0)executor.fault("flow_config");
  pinMode(FLOW_GPIO,INPUT_PULLUP);attachInterrupt(digitalPinToInterrupt(FLOW_GPIO),pulse,RISING);
 }
 WiFi.mode(WIFI_STA);WiFi.begin(WIFI_SSID,WIFI_PASSWORD);configTime(0,0,"pool.ntp.org","time.google.com");
 mqtt.begin(MQTT_HOST,MQTT_PORT,network);mqtt.onMessage(receive);mqtt.setOptions(15,true,100);
 esp_task_wdt_init(5,true);esp_task_wdt_add(nullptr);
 Serial.printf("Полив %s: выходы %s, flash %s\n",box.c_str(),OUTPUTS_VERIFIED?"разрешены":"заблокированы",journal.healthy?"OK":"ошибка");
}
void loop() {
 static uint32_t reconnect=0,lastStatus=0,lastFlow=0,lastCount=0,opened=0,lastPulse=0;
 static uint8_t previous=0;
 uint32_t now=mono();
 if(OUTPUTS_VERIFIED)executor.tick(time(nullptr),now,validTime());
 uint8_t zone=executor.zone();uint32_t count=pulses();
 if(zone!=previous){opened=now;lastPulse=now;previous=zone;status();}
 if(count!=lastCount)lastPulse=now;
 if(FLOW_ENABLED && zone && now-opened>=10 && now-lastPulse>=10)executor.fault("no_flow");
 if(now-lastFlow>=5) {
  uint32_t delta=count-lastCount;
  lpm=FLOW_ENABLED?delta*60.0f/FLOW_PULSES_PER_LITER/std::max(1u,now-lastFlow):0;
  liters=FLOW_ENABLED?count/FLOW_PULSES_PER_LITER:0;
  if(FLOW_ENABLED && !zone && delta>0)executor.fault("unexpected_flow");
  lastCount=count;lastFlow=now;
  JsonDocument d;base(d);d["flow_enabled"]=FLOW_ENABLED;d["instant_lpm"]=lpm;d["total_liters"]=liters;
  auto a=d["active_zones"].to<JsonArray>();if(zone)a.add(zone);publish("flow",d);
 }
 if(now-lastStatus>=30){status();lastStatus=now;}
 // Connect and drain only with valves closed; slow network calls cannot extend watering.
 if(!executor.zone()) {
  if(WiFi.status()==WL_CONNECTED && !mqtt.connected() && now-reconnect>=5){reconnect=now;connectMqtt();}
  if(!journal.healthy && executor.error().empty())executor.fault("storage_error");
  journal.drain();
 }
 if(mqtt.connected())mqtt.loop();
 if(!incoming.empty()) {auto msg=std::move(incoming.front());incoming.pop_front();process(std::move(msg));}
 esp_task_wdt_reset();delay(2);
}
