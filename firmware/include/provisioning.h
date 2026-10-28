#pragma once
#include <ArduinoJson.h>
#include <string>
namespace poliv {
struct NetworkConfig {
 std::string ssid, wifiPassword, host, user, password, ca;
 uint16_t port=1883;
 bool tls=false;
};
bool decodeNetwork(JsonDocument& document, NetworkConfig& config);
}
