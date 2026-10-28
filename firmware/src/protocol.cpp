#include <algorithm>
#include "protocol.h"
#include <cstring>
#include <cstdio>
#include <ctime>
using namespace poliv;
// Decode server date + local minute to UTC; reject normalized invalid dates.
bool poliv::decode(JsonDocument& d,Schedule& s,const std::string& box) {
 if(d["protocol_version"]!="1.0" || d["box_id"].as<std::string>()!=box ||
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
namespace {
void canonical(JsonVariantConst value,std::string& out,int context=0) {
 if(value.is<JsonObjectConst>()) {
  std::vector<std::string> keys;
  for(JsonPairConst p:value.as<JsonObjectConst>()) {
   std::string k=p.key().c_str();
   if((context==1 && (k=="generated_ts" || k=="schedule_hash")) || (context==3 && k=="run_id"))continue;
   keys.push_back(k);
  }
  std::sort(keys.begin(),keys.end());out+='{' ;bool first=true;
  for(auto& k:keys) {
   if(!first) {out+=',';}
   first=false;
   JsonDocument key;key.set(k);std::string quoted;serializeJson(key,quoted);out+=quoted;out+=':';
   // Server computes hash before persist_schedule assigns its transport version.
   if(context==1 && k=="schedule_version")out+='0';
   else canonical(value[k],out,context==1 && k=="runs"?2:0);
  }
  out+='}';
 } else if(value.is<JsonArrayConst>()) {
  out+='[';bool first=true;for(auto v:value.as<JsonArrayConst>()) {
   if(!first) {out+=',';}
   first=false;canonical(v,out,context==2?3:0);
  }out+=']';
 } else {std::string scalar;serializeJson(value,scalar);out+=scalar;}
}
}
std::string poliv::canonicalContent(JsonDocument& d) { std::string out;canonical(d.as<JsonVariantConst>(),out,1);return out; }
