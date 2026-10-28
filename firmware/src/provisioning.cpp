#include "provisioning.h"
#include <cctype>
bool poliv::decodeNetwork(JsonDocument& d, NetworkConfig& output) {
 if(!d["ssid"].is<std::string>() || !d["wifi_password"].is<std::string>() ||
    !d["host"].is<std::string>() || !d["username"].is<std::string>() ||
    !d["password"].is<std::string>() || !d["port"].is<unsigned>() ||
    !d["tls"].is<bool>())return false;
 NetworkConfig c;
 c.ssid=d["ssid"].as<std::string>();c.wifiPassword=d["wifi_password"].as<std::string>();
 c.host=d["host"].as<std::string>();c.user=d["username"].as<std::string>();
 c.password=d["password"].as<std::string>();c.ca=d["ca_pem"] | "";
 unsigned port=d["port"];c.tls=d["tls"];
 if(c.ssid.empty() || c.ssid.size()>32 || c.wifiPassword.size()>63 ||
    c.host.empty() || c.host.size()>253 || c.user.empty() || c.user.size()>96 ||
    c.password.empty() || c.password.size()>256 || !port || port>65535 ||
    c.ca.size()>6000)return false;
 for(unsigned char ch:c.host)if(!std::isalnum(ch) && ch!='.' && ch!='-' && ch!=':')return false;
 for(auto* text:{&c.ssid,&c.wifiPassword,&c.user,&c.password})
  if(text->find('\0')!=std::string::npos)return false;
 if(c.tls && (c.ca.find("-----BEGIN CERTIFICATE-----")==std::string::npos ||
              c.ca.find("-----END CERTIFICATE-----")==std::string::npos))return false;
 c.port=port;output=std::move(c);return true;
}
