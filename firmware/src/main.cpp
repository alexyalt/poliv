#include <Arduino.h>
#include <ArduinoJson.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include "provisioning.h"
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
#include "protocol.h"
#include <mbedtls/sha256.h>
using namespace poliv;
constexpr size_t MaxPayload = 48 * 1024;
constexpr size_t MaxEvents = 64;
String box, prefix;
uint32_t mono() { return uint32_t(esp_timer_get_time() / 1000000); }
bool validTime() { return time(nullptr) > 1735689600; }
class Relays : public IRelayDriver {
 public:
  void begin() {
    pinMode(13,OUTPUT);pinMode(12,OUTPUT);pinMode(14,OUTPUT);select(0);
    esp_timer_create_args_t args={};args.callback=[](void* self){static_cast<Relays*>(self)->check();};
    args.arg=this;args.name="valve-deadline";
    if(esp_timer_create(&args,&timer)!=ESP_OK || esp_timer_start_periodic(timer,25000)!=ESP_OK) {
      enabled=false;
    }
  }
  void setTimeout(uint32_t seconds) override { timeout=seconds; }
  void select(uint8_t zone) override {
    portENTER_CRITICAL(&mux);
    if(!enabled || !OUTPUTS_VERIFIED)zone=0;
    deadline=zone ? esp_timer_get_time()+int64_t(timeout)*1000000 : 0;
    write(zone);portEXIT_CRITICAL(&mux);
  }
  volatile bool expired=false;
  bool enabled=true;
 private:
  portMUX_TYPE mux=portMUX_INITIALIZER_UNLOCKED;
  esp_timer_handle_t timer=nullptr;
  uint32_t timeout=0;
  int64_t deadline=0;
  void write(uint8_t zone) {
    uint16_t bits=relayBits(zone,RELAY_ACTIVE_HIGH);
    digitalWrite(14,LOW);shiftOut(13,12,MSBFIRST,bits >> 8);
    shiftOut(13,12,MSBFIRST,bits & 255);digitalWrite(14,HIGH);
  }
  void check() {
    portENTER_CRITICAL(&mux);
    if(deadline && esp_timer_get_time()>=deadline) {write(0);deadline=0;expired=true;}
    portEXIT_CRITICAL(&mux);
  }
} relays;
WiFiClient network;
WiFiClientSecure secureNetwork;
NetworkConfig networkConfig;
constexpr size_t MaxInbound=16*1024;
MQTTClient mqtt(MaxInbound + 1024);
Preferences nvs;
int64_t pauseUntil=0;
String uid() { char b[25];snprintf(b,sizeof(b),"%08lx%08lx%08lx",(unsigned long)esp_random(),(unsigned long)esp_random(),(unsigned long)esp_random());return String(b); }
void base(JsonDocument& d) { d["protocol_version"]="1.0";d["box_id"]=box;d["ts"]=int64_t(time(nullptr)); }
bool publish(const char* kind, JsonDocument& d, bool retained=false) {
 String raw;serializeJson(d,raw);return mqtt.connected() && mqtt.publish(prefix+kind,raw,retained,1);
}
// LittleFS atomic rename + CRC detects corruption; no automatic formatting.
uint32_t crc(const String& raw) {
 uint32_t c=0xffffffff;
 for(size_t i=0;i<raw.length();++i) {
  c^=uint8_t(raw[i]);for(int bit=0;bit<8;++bit)c=(c>>1)^(0xedb88320u&uint32_t(-int32_t(c&1)));
 }return ~c;
}
bool atomicJson(const char* path, JsonDocument& d) {
 String body;serializeJson(d,body);if(body.length()>MaxPayload)return false;
 JsonDocument envelope;envelope["body"]=body;envelope["crc32"]=crc(body);
 String tmp=String(path)+".tmp";File f=LittleFS.open(tmp,"w");if(!f)return false;
 size_t written=serializeJson(envelope,f);f.flush();f.close();
 if(written!=measureJson(envelope)){LittleFS.remove(tmp);return false;}
 return LittleFS.rename(tmp,path);
}
bool readJson(const char* path, JsonDocument& d) {
 File f=LittleFS.open(path,"r");if(!f || f.size()>MaxPayload*2+128)return false;
 JsonDocument envelope;if(deserializeJson(envelope,f))return false;
 if(!envelope["body"].is<const char*>() || !envelope["crc32"].is<uint32_t>())return false;
 String body=envelope["body"].as<String>();
 return body.length()<=MaxPayload && crc(body)==envelope["crc32"].as<uint32_t>() && !deserializeJson(d,body);
}
uint32_t pulses();
class Journal : public IJournal {
 public:
  std::deque<String> events;
  bool healthy=false;
  uint32_t startPulses=0;
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
    int64_t end=time(nullptr);d["start_ts"]=end-seconds;
    if(status!="started") {
      d["end_ts"]=end;d["water_sec"]=seconds;
      if(FLOW_ENABLED)d["volume_liters"]=(pulses()-startPulses)/FLOW_PULSES_PER_LITER;
      else d["volume_liters"]=nullptr;
    } else startPulses=pulses();
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
bool verifyHash(JsonDocument& d) {
 std::string canonical=canonicalContent(d);unsigned char digest[32];char hex[65];
 mbedtls_sha256_ret(reinterpret_cast<const unsigned char*>(canonical.data()),canonical.size(),digest,0);
 for(int i=0;i<32;++i)snprintf(hex+i*2,3,"%02x",digest[i]);
 return d["schedule_hash"].is<const char*>() && strcmp(hex,d["schedule_hash"].as<const char*>())==0;
}
void status() {
 JsonDocument d;base(d);d["online"]=true;d["mode"]=!OUTPUTS_VERIFIED ? "service" : executor.mode();d["phase"]=executor.phase()=="gap" ? "waiting" : executor.phase();
 auto a=d["active_zones"].to<JsonArray>();auto b=d["display_zones"].to<JsonArray>();
 if(executor.zone()){a.add(executor.zone());b.add(executor.zone());d["primary_zone"]=executor.zone();}
 else d["primary_zone"]=nullptr;
 d["schedule_version"]=executor.schedule().version;d["schedule_hash"]=executor.schedule().hash;
 d["pause_until_ts"]=pauseUntil ? pauseUntil : 0;
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
 if(raw.length()>MaxInbound)return;
 if(incoming.size()>=2){executor.fault("inbound_overflow");return;}
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
  Schedule s;bool ok=decode(d,s,box.c_str()) && verifyHash(d);
  if(ok && s.version==executor.schedule().version) {
   ok=s.hash==executor.schedule().hash && s.runs.size()==executor.schedule().runs.size();
   if(ok)for(size_t i=0;i<s.runs.size();++i)if(s.runs[i].id!=executor.schedule().runs[i].id)ok=false;
  }
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
  else if(cmd=="pause_controller") {
   int64_t until=p["until_ts"] | int64_t(0);
   if(p["duration_sec"].is<uint32_t>() && validTime())until=time(nullptr)+p["duration_sec"].as<uint32_t>();
   if(validTime() && until>time(nullptr) && until-time(nullptr)<=604800) {
    executor.pause();pauseUntil=until;
    ok=nvs.putLong64("pause_until",pauseUntil)>0 && nvs.putBool("paused",true)>0;
   }
  }
  else if(cmd=="resume_controller"){ok=nvs.putBool("paused",false)>0;if(ok){pauseUntil=0;nvs.putLong64("pause_until",0);executor.resume();}}
  else if(cmd=="clear_error"){ok=executor.clearFault();if(ok)nvs.putString("fault","");}
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
 if(networkConfig.tls && !validTime())return;
 if(!mqtt.connect(("poliv-"+box).c_str(),networkConfig.user.c_str(),networkConfig.password.c_str()))return;
 mqtt.subscribe(prefix+"command",1);mqtt.subscribe(prefix+"schedule",1);mqtt.subscribe(prefix+"time",1);
 JsonDocument d;base(d);d["online"]=true;publish("lwt",d,true);
 d.clear();base(d);d["firmware_version"]="0.11.0";d["hardware_model"]="ESP32-DevKit-74HC595";
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
 if(!nvs.begin("poliv",false))executor.fault("storage_error");
 if(!journal.load())executor.fault("storage_error");
 if(journal.healthy) {
  JsonDocument d;
  if(LittleFS.exists("/schedule.json")) {
   Schedule s;
   if(!readJson("/schedule.json",d) || (!decode(d,s,box.c_str()) || !verifyHash(d)))executor.fault("invalid_schedule");
   else {
    // Read cursor before apply() creates an initial checkpoint.
    JsonDocument c;bool exists=LittleFS.exists("/cursor.json");bool valid=exists && readJson("/cursor.json",c);
    journal.version=s.version;
    if(executor.apply(s,false)) {
     if(!exists || !valid)executor.fault("invalid_cursor");
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
 String latch=nvs.getString("fault","");if(latch.length())executor.fault(latch.c_str());
 pauseUntil=nvs.getLong64("pause_until",0);
 if(nvs.getBool("paused",false))executor.pause();
 if(!relays.enabled)executor.fault("deadline_timer");
 if(FLOW_ENABLED) {
  if(FLOW_PULSES_PER_LITER<=0)executor.fault("flow_config");
  pinMode(FLOW_GPIO,INPUT_PULLUP);attachInterrupt(digitalPinToInterrupt(FLOW_GPIO),pulse,RISING);
 }
 networkConfig={WIFI_SSID,WIFI_PASSWORD,MQTT_HOST,MQTT_USER,MQTT_PASSWORD,"",MQTT_PORT,false};
 String savedNetwork=nvs.getString("network","");
 if(savedNetwork.length()) {
  JsonDocument configDocument;
  if(deserializeJson(configDocument,savedNetwork) || !decodeNetwork(configDocument,networkConfig))executor.fault("network_config");
 }
 if(networkConfig.tls)secureNetwork.setCACert(networkConfig.ca.c_str());
 WiFi.mode(WIFI_STA);WiFi.begin(networkConfig.ssid.c_str(),networkConfig.wifiPassword.c_str());configTime(0,0,"pool.ntp.org","time.google.com");
 if(networkConfig.tls)mqtt.begin(networkConfig.host.c_str(),networkConfig.port,secureNetwork);
 else mqtt.begin(networkConfig.host.c_str(),networkConfig.port,network);mqtt.onMessage(receive);mqtt.setOptions(15,true,100);
 esp_task_wdt_init(5,true);esp_task_wdt_add(nullptr);
 Serial.printf("Полив %s: выходы %s, flash %s\n",box.c_str(),OUTPUTS_VERIFIED?"разрешены":"заблокированы",journal.healthy?"OK":"ошибка");
}
void loop() {
 static uint32_t reconnect=0,lastStatus=0,lastFlow=0,lastCount=0,opened=0,lastPulse=0;
 static uint8_t previous=0;
 static uint32_t observed=0;
 uint32_t now=mono();
 if(executor.mode()=="paused" && pauseUntil && validTime() && time(nullptr)>=pauseUntil) {
  if(nvs.putBool("paused",false)>0){pauseUntil=0;nvs.putLong64("pause_until",0);executor.resume();}
 }
 std::string oldRun=executor.cursor().run;auto oldStep=executor.cursor().step;
 if(OUTPUTS_VERIFIED)executor.tick(time(nullptr),now,validTime());
 if(relays.expired) {
  relays.expired=false;
  if(executor.zone() && executor.cursor().run==oldRun && executor.cursor().step==oldStep)executor.fault("valve_timeout");
 }
 uint8_t zone=executor.zone();uint32_t count=pulses();
 if(zone!=previous){opened=now;lastPulse=now;previous=zone;status();}
 if(count!=observed){lastPulse=now;observed=count;}
 if(FLOW_ENABLED && zone && now-opened>=10 && now-lastPulse>=10)executor.fault("no_flow");
 if(now-lastFlow>=5) {
  uint32_t delta=count-lastCount;
  lpm=FLOW_ENABLED?delta*60.0f/FLOW_PULSES_PER_LITER/std::max(1u,now-lastFlow):0;
  liters=FLOW_ENABLED?count/FLOW_PULSES_PER_LITER:0;
  if(FLOW_ENABLED && !zone && now-opened>=10 && delta>0)executor.fault("unexpected_flow");
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
 static String serialCommand;
 static bool serialOverflow=false;
 while(Serial.available()) {
  char c=Serial.read();
  if(c=='\n') {
   serialCommand.trim();
   if(!serialOverflow && serialCommand.startsWith("CONFIG ") && !OUTPUTS_VERIFIED) {
    executor.stop();JsonDocument configuration;NetworkConfig proposed;
    if(!deserializeJson(configuration,serialCommand.substring(7)) && decodeNetwork(configuration,proposed)) {
     String raw;serializeJson(configuration,raw);
     if(nvs.putString("network",raw)==raw.length()){Serial.println("Конфигурация сохранена; перезапуск");ESP.restart();}
     else Serial.println("Ошибка сохранения конфигурации");
    } else Serial.println("Некорректная конфигурация");
   }
   if(serialCommand=="NETWORK_RESET" && !OUTPUTS_VERIFIED){executor.stop();nvs.remove("network");ESP.restart();}
   if(serialCommand=="FORMAT" && !OUTPUTS_VERIFIED) {executor.stop();LittleFS.end();if(LittleFS.format())ESP.restart();}
   serialCommand="";serialOverflow=false;
  } else if(serialCommand.length()<8192)serialCommand+=c;else serialOverflow=true;
 }
 static std::string savedFault;
 if(executor.error()!=savedFault){savedFault=executor.error();nvs.putString("fault",savedFault.c_str());status();}
 esp_task_wdt_reset();delay(2);
}
