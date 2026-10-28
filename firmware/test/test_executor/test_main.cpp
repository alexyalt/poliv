#include <unity.h>
#include "executor.h"
#include "protocol.h"
#include "provisioning.h"
#include <fstream>
#include <sstream>
#include <cstdlib>
using namespace poliv;
struct Relay : IRelayDriver { uint8_t zone = 99; void select(uint8_t z) override {zone=z;} };
struct Journal : IJournal {
 bool writable=true, eventsWritable=true; int events=0; Cursor saved; std::vector<std::string> done;
 bool save(const Cursor& c,const std::vector<std::string>& d) override {saved=c;done=d;return writable;}
 bool event(const std::string&,uint8_t,const std::string&,const std::string&,uint32_t) override {++events;return writable && eventsWritable;}
};
Schedule plan() {return {1,"hash",120,{{"r1",100,{{1,10,"water"},{0,5,"soak"},{2,7,"water"}}}}};}
void setUp() {} void tearDown() {}
void offline_execution() {
 Relay r; Journal j; Executor e(r,j); TEST_ASSERT_EQUAL(0,r.zone); TEST_ASSERT_TRUE(e.apply(plan()));
 e.tick(100,0,false); TEST_ASSERT_EQUAL(0,r.zone);
 e.tick(100,1,true); TEST_ASSERT_EQUAL(1,r.zone);
 e.tick(99999,6,false); TEST_ASSERT_EQUAL(1,r.zone); // wall clock jump has no effect
 e.tick(0,11,false); TEST_ASSERT_EQUAL(0,r.zone); TEST_ASSERT_EQUAL_STRING("soak",e.phase().c_str());
 e.tick(0,16,false); TEST_ASSERT_EQUAL(2,r.zone);
 e.tick(100,23,false); TEST_ASSERT_EQUAL(0,r.zone);
 e.tick(100,24,true); TEST_ASSERT_EQUAL(0,r.zone); TEST_ASSERT_EQUAL(4,j.events);
}
void manual_pause_stop() {
 Relay r; Journal j; Executor e(r,j); e.apply(plan());e.tick(100,0,true);e.tick(101,3,true);
 TEST_ASSERT_TRUE(e.manual(4,2,3)); TEST_ASSERT_EQUAL(4,r.zone);e.tick(102,5,true);TEST_ASSERT_EQUAL(0,r.zone);
 e.tick(103,6,true); TEST_ASSERT_EQUAL(1,r.zone);TEST_ASSERT_EQUAL(7,e.cursor().remaining);
 e.pause();TEST_ASSERT_EQUAL(0,r.zone);e.tick(110,50,true);e.resume();e.tick(110,51,true);
 TEST_ASSERT_EQUAL(1,r.zone);TEST_ASSERT_EQUAL(7,e.cursor().remaining);e.stop();TEST_ASSERT_EQUAL(0,r.zone);
 e.tick(100,52,true);TEST_ASSERT_EQUAL(0,r.zone);
}
void invalid_and_storage() {
 Relay r;Journal j;Executor e(r,j);auto p=plan();p.runs[0].steps[0].zone=17;TEST_ASSERT_FALSE(e.apply(p));
 TEST_ASSERT_FALSE(e.manual(1,3601,0));TEST_ASSERT_FALSE(e.manual(0,10,0));
 TEST_ASSERT_TRUE(e.apply(plan()));j.writable=false;e.tick(100,0,true);TEST_ASSERT_EQUAL(0,r.zone);
 TEST_ASSERT_EQUAL_STRING("storage_error",e.error().c_str());TEST_ASSERT_FALSE(e.manual(1,10,0));
}
void recovery_skip_resume() {
 Relay r;Journal j;Executor e(r,j);e.apply(plan());e.restore({"r1",0,4,true},{},true);
 e.tick(100,0,false);TEST_ASSERT_EQUAL(0,r.zone);e.tick(100,1,true);TEST_ASSERT_EQUAL(1,r.zone);
 e.tick(100,5,true);TEST_ASSERT_EQUAL(0,r.zone);
 Relay r2;Journal j2;Executor e2(r2,j2);e2.apply(plan());e2.restore({"r1",0,4,true},{},false);
 e2.tick(100,0,true);TEST_ASSERT_EQUAL(0,r2.zone);TEST_ASSERT_EQUAL(1,j2.done.size());
}
void fault_late_and_rollover() {
 Relay r;Journal j;Executor e(r,j);e.apply(plan());e.tick(300,0,true);TEST_ASSERT_EQUAL(0,r.zone);
 TEST_ASSERT_TRUE(e.manual(16,5,0xfffffffe));e.tick(300,1,true);TEST_ASSERT_EQUAL(2,e.cursor().remaining);
 e.fault("no_flow");TEST_ASSERT_EQUAL(0,r.zone);TEST_ASSERT_FALSE(e.manual(2,1,2));
 TEST_ASSERT_TRUE(e.clearFault());TEST_ASSERT_TRUE(e.manual(2,1,2));
 TEST_ASSERT_EQUAL_HEX16(0x8000,relayBits(16,true));TEST_ASSERT_EQUAL_HEX16(0xffff,relayBits(0,false));
}
std::string fixture(const char* path) {std::ifstream f(path);std::stringstream s;s<<f.rdbuf();return s.str();}
void server_contract_and_canonical_hash() {
 setenv("TZ","UTC0",1);tzset();
 JsonDocument d;TEST_ASSERT_FALSE(bool(deserializeJson(d,fixture("test/fixtures/schedule.json"))));
 Schedule s;TEST_ASSERT_TRUE(decode(d,s,"5B2338"));TEST_ASSERT_EQUAL(1,s.version);
 TEST_ASSERT_EQUAL(2,s.runs.size());TEST_ASSERT_EQUAL(1,s.runs[0].steps[0].zone);
 TEST_ASSERT_EQUAL(60,s.runs[0].steps[0].seconds);TEST_ASSERT_EQUAL(0,s.runs[0].steps[1].zone);
 TEST_ASSERT_EQUAL_INT64(1793156400,s.runs[0].start);
 TEST_ASSERT_EQUAL_STRING(fixture("test/fixtures/canonical.json").c_str(),canonicalContent(d).c_str());
}
void reject_bad_wire_payload() {
 JsonDocument d;deserializeJson(d,fixture("test/fixtures/schedule.json"));
 Schedule s;TEST_ASSERT_FALSE(decode(d,s,"OTHER"));
 d["runs"][0]["steps"][0]["active_zones"].as<JsonArray>().add(2);
 s={};TEST_ASSERT_FALSE(decode(d,s,"5B2338"));
 deserializeJson(d,fixture("test/fixtures/schedule.json"));d["runs"][0]["date"]="2026-02-30";
 s={};TEST_ASSERT_FALSE(decode(d,s,"5B2338"));
 deserializeJson(d,fixture("test/fixtures/schedule.json"));d["runs"][0]["steps"][0]["duration_sec"]=-1;
 s={};TEST_ASSERT_FALSE(decode(d,s,"5B2338"));
}
void event_failure_prevents_opening() {
 Relay r;Journal j;Executor e(r,j);TEST_ASSERT_TRUE(e.apply(plan()));j.eventsWritable=false;
 e.tick(100,0,true);TEST_ASSERT_EQUAL(0,r.zone);TEST_ASSERT_EQUAL_STRING("event_buffer_full",e.error().c_str());
 TEST_ASSERT_FALSE(e.clearFault());
}
void boot_load_does_not_overwrite_cursor() {
 Relay r;Journal j;j.saved={"r1",0,4,true};Executor e(r,j);
 TEST_ASSERT_TRUE(e.apply(plan(),false));TEST_ASSERT_EQUAL(4,j.saved.remaining);
 e.restore(j.saved,{},true);e.tick(100,0,true);TEST_ASSERT_EQUAL(4,e.cursor().remaining);
 auto old=plan();old.version=0;TEST_ASSERT_FALSE(e.apply(old));
 auto newer=plan();newer.version=2;TEST_ASSERT_FALSE(e.apply(newer));
}
void provisioning_validation() {
 JsonDocument d;deserializeJson(d,R"({"ssid":"test","wifi_password":"secret123","host":"broker.local","username":"box_5B2338","password":"password","port":8883,"tls":true})");
 NetworkConfig c;c.host="unchanged";
 TEST_ASSERT_FALSE(decodeNetwork(d,c));TEST_ASSERT_EQUAL_STRING("unchanged",c.host.c_str());
 d["ca_pem"]="-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----";
 TEST_ASSERT_TRUE(decodeNetwork(d,c));TEST_ASSERT_TRUE(c.tls);TEST_ASSERT_EQUAL(8883,c.port);
 d["port"]=65536;TEST_ASSERT_FALSE(decodeNetwork(d,c));
 d["port"]=1883;d["tls"]=false;d["host"]="mqtt://host/path";TEST_ASSERT_FALSE(decodeNetwork(d,c));
 d["host"]="192.168.1.2";TEST_ASSERT_TRUE(decodeNetwork(d,c));
}
void server_envelope_without_box() {
 JsonDocument d;deserializeJson(d,R"({"protocol_version":"1.0","command":"ping","command_id":"cid","ts":1})");
 TEST_ASSERT_TRUE(acceptsEnvelope(d,"poliv/5B2338/command","5B2338"));
 TEST_ASSERT_TRUE(acceptsEnvelope(d,"poliv/5B2338/time","5B2338"));
 TEST_ASSERT_FALSE(acceptsEnvelope(d,"poliv/OTHER/command","5B2338"));
 d["box_id"]="OTHER";TEST_ASSERT_FALSE(acceptsEnvelope(d,"poliv/5B2338/command","5B2338"));
 d["box_id"]="5B2338";TEST_ASSERT_TRUE(acceptsEnvelope(d,"poliv/5B2338/command","5B2338"));
}
int main() {UNITY_BEGIN();RUN_TEST(server_envelope_without_box);RUN_TEST(provisioning_validation);RUN_TEST(event_failure_prevents_opening);RUN_TEST(boot_load_does_not_overwrite_cursor);RUN_TEST(server_contract_and_canonical_hash);RUN_TEST(reject_bad_wire_payload);RUN_TEST(offline_execution);RUN_TEST(manual_pause_stop);RUN_TEST(invalid_and_storage);RUN_TEST(recovery_skip_resume);RUN_TEST(fault_late_and_rollover);return UNITY_END();}
