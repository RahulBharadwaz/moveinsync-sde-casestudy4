#!/usr/bin/env bash
# MoveInSync Case Study 4: Live REST Ingestion & Telemetry Demo
set -e

BASE_URL="http://localhost:8080/api"

echo "=========================================================="
echo "    MoveInSync Case Study 4: Live API Demonstration       "
echo "=========================================================="

echo -e "\n[1/5] Ingesting GPS Telemetry Location Update..."
curl -s -X POST "$BASE_URL/events/location" \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "trip:TRIP-101",
    "cabId": "CAB-01",
    "latitude": 12.9716,
    "longitude": 77.5946,
    "speedKmph": 45.0
  }' | jq . || true

echo -e "\n[2/5] Ingesting Trip Status Transition (CAB_ARRIVED)..."
curl -s -X POST "$BASE_URL/events/status" \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "trip:TRIP-101",
    "tripId": "TRIP-101",
    "status": "CAB_ARRIVED",
    "delayMinutes": 0
  }' | jq . || true

echo -e "\n[3/5] Triggering Emergency SOS Panic Button Alert (Priority Lane 0)..."
curl -s -X POST "$BASE_URL/events/sos" \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "trip:TRIP-101",
    "tripId": "TRIP-101",
    "cabId": "CAB-01",
    "employeeId": "emp-1",
    "alertType": "PANIC_BUTTON",
    "severity": "CRITICAL",
    "message": "Passenger triggered emergency button"
  }' | jq . || true

echo -e "\n[4/5] Fetching Consolidated Topic Snapshot for 'trip:TRIP-101'..."
curl -s "$BASE_URL/topics/trip:TRIP-101/snapshot" | jq . || true

echo -e "\n[5/5] Inspecting 3 AM On-Call Metrics..."
curl -s "$BASE_URL/metrics" | jq . || true

echo -e "\n=========================================================="
echo "    Demo Completed Successfully!                          "
echo "=========================================================="

