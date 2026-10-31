# MoveInSync Case Study 4: Live REST Ingestion & Telemetry Demo (PowerShell)
$BaseUrl = "http://localhost:8080/api"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "    MoveInSync Case Study 4: Live API Demonstration       " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

Write-Host "`n[1/5] Ingesting GPS Telemetry Location Update..." -ForegroundColor Yellow
$locBody = @{
    topic = "trip:TRIP-101"
    cabId = "CAB-01"
    latitude = 12.9716
    longitude = 77.5946
    speedKmph = 45.0
} | ConvertTo-Json
Invoke-RestMethod -Uri "$BaseUrl/events/location" -Method Post -ContentType "application/json" -Body $locBody | ConvertTo-Json

Write-Host "`n[2/5] Ingesting Trip Status Transition (CAB_ARRIVED)..." -ForegroundColor Yellow
$statusBody = @{
    topic = "trip:TRIP-101"
    tripId = "TRIP-101"
    status = "CAB_ARRIVED"
    delayMinutes = 0
} | ConvertTo-Json
Invoke-RestMethod -Uri "$BaseUrl/events/status" -Method Post -ContentType "application/json" -Body $statusBody | ConvertTo-Json

Write-Host "`n[3/5] Triggering Emergency SOS Panic Button Alert (Priority Lane 0)..." -ForegroundColor Red
$sosBody = @{
    topic = "trip:TRIP-101"
    tripId = "TRIP-101"
    cabId = "CAB-01"
    employeeId = "emp-1"
    alertType = "PANIC_BUTTON"
    severity = "CRITICAL"
    message = "Passenger triggered emergency button"
} | ConvertTo-Json
Invoke-RestMethod -Uri "$BaseUrl/events/sos" -Method Post -ContentType "application/json" -Body $sosBody | ConvertTo-Json

Write-Host "`n[4/5] Fetching Consolidated Topic Snapshot for 'trip:TRIP-101'..." -ForegroundColor Green
Invoke-RestMethod -Uri "$BaseUrl/topics/trip:TRIP-101/snapshot" -Method Get | ConvertTo-Json

Write-Host "`n[5/5] Inspecting 3 AM On-Call Metrics..." -ForegroundColor Magenta
Invoke-RestMethod -Uri "$BaseUrl/metrics" -Method Get | ConvertTo-Json

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "    Demo Completed Successfully!                          " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
