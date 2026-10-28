#pragma once
#include <ArduinoJson.h>
#include "executor.h"
namespace poliv {
bool decode(JsonDocument& d, Schedule& s, const std::string& box);
std::string canonicalContent(JsonDocument& d);
}
