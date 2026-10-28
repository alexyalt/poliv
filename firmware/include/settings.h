#pragma once
// Copy secrets.example.h to secrets.h for local credentials and verified settings.
#if __has_include("secrets.h")
#include "secrets.h"
#endif
#ifndef WIFI_SSID
#define WIFI_SSID ""
#endif
#ifndef WIFI_PASSWORD
#define WIFI_PASSWORD ""
#endif
#ifndef MQTT_HOST
#define MQTT_HOST "192.168.1.2"
#endif
#ifndef MQTT_PORT
#define MQTT_PORT 1883
#endif
#ifndef MQTT_USER
#define MQTT_USER ""
#endif
#ifndef MQTT_PASSWORD
#define MQTT_PASSWORD ""
#endif
#ifndef RELAY_ACTIVE_HIGH
#define RELAY_ACTIVE_HIGH true
#endif
#ifndef OUTPUTS_VERIFIED
#define OUTPUTS_VERIFIED false
#endif
#ifndef FLOW_ENABLED
#define FLOW_ENABLED false
#endif
#ifndef FLOW_PULSES_PER_LITER
#define FLOW_PULSES_PER_LITER 450.0f
#endif
#ifndef FLOW_GPIO
#define FLOW_GPIO 27
#endif
#ifndef RESUME_AFTER_POWER_LOSS
#define RESUME_AFTER_POWER_LOSS false
#endif
