#pragma once
#include <ArduinoJson.h>
#include "executor.h"
namespace poliv {
bool decode(JsonDocument& d, Schedule& s, const std::string& box);
std::string canonicalContent(JsonDocument& d);
}
namespace poliv {
// Server command/time envelopes address devices by topic; box_id in body is optional.
bool acceptsEnvelope(JsonDocument& document, const std::string& topic,
                     const std::string& box);
}
