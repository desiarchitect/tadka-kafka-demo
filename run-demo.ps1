# Tadka Kafka Demo Runner — Windows PowerShell
# Usage: .\run-demo.ps1

$ErrorActionPreference = "Stop"

function Write-Header {
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  🍛 Tadka Kafka Demo — The Desi Architect" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""
}

function Start-Kafka {
    Write-Host "  Starting Kafka & Kafka UI..." -ForegroundColor Yellow
    docker compose up -d
    Write-Host "  Waiting for Kafka to be ready..." -ForegroundColor Yellow
    
    $retries = 0
    $maxRetries = 30
    while ($retries -lt $maxRetries) {
        $health = docker inspect --format='{{.State.Health.Status}}' demo-kafka-1 2>$null
        if ($health -eq "healthy") {
            Write-Host "  ✅ Kafka is ready!" -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 2
        $retries++
        Write-Host "  ⏳ Waiting... ($retries/$maxRetries)" -ForegroundColor Gray
    }
    Write-Host "  ⚠️  Kafka may not be fully ready. Proceeding anyway." -ForegroundColor Yellow
}

function Show-Menu {
    Write-Host ""
    Write-Host "  Pick a demo:" -ForegroundColor White
    Write-Host ""
    Write-Host "  1) Setup — Create topics" -ForegroundColor White
    Write-Host "  2) Producer — Send orders" -ForegroundColor White
    Write-Host "  3) Consumers — Run all 4 consumers" -ForegroundColor White
    Write-Host "  4) Partition Demo — Key routing & ordering" -ForegroundColor White
    Write-Host "  5) Scaling Demo — Consumer golden rule" -ForegroundColor White
    Write-Host "  6) Hot Partition — Skew + compound key fix" -ForegroundColor White
    Write-Host "  7) Delivery Guarantees — At-most/least/idempotent" -ForegroundColor White
    Write-Host "  8) Offset Reset — Replay from beginning" -ForegroundColor White
    Write-Host "  9) Open Kafka UI" -ForegroundColor White
    Write-Host "  0) Stop & cleanup" -ForegroundColor White
    Write-Host ""
}

# Main
Write-Header

# Check prerequisites
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "  ❌ Docker not found. Install Docker Desktop first." -ForegroundColor Red
    exit 1
}
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
    Write-Host "  ❌ Node.js not found. Install Node.js 18+ first." -ForegroundColor Red
    exit 1
}

# Install dependencies if needed
if (-not (Test-Path "node_modules")) {
    Write-Host "  Installing dependencies..." -ForegroundColor Yellow
    npm install
}

# Start Kafka
Start-Kafka

while ($true) {
    Show-Menu
    $choice = Read-Host "  Enter choice"
    
    switch ($choice) {
        "1" { npm run setup }
        "2" { npm run producer }
        "3" {
            Write-Host "  Starting all 4 consumers in new windows..." -ForegroundColor Yellow
            Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; npm run notification"
            Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; npm run analytics"
            Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; npm run restaurant"
            Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; npm run search-indexer"
            Write-Host "  ✅ 4 consumer windows opened" -ForegroundColor Green
        }
        "4" { npm run partition-demo }
        "5" {
            Write-Host "  Starting 3 consumer instances..." -ForegroundColor Yellow
            Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; node scaling-demo.js 1"
            Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; node scaling-demo.js 2"
            Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; node scaling-demo.js 3"
            Write-Host "  ✅ 3 instances started. Try starting a 4th: node scaling-demo.js 4" -ForegroundColor Green
        }
        "6" { npm run hot-partition }
        "7" {
            Write-Host ""
            Write-Host "  a) At-Most-Once" -ForegroundColor White
            Write-Host "  b) At-Least-Once" -ForegroundColor White
            Write-Host "  c) Idempotent Consumer" -ForegroundColor White
            Write-Host ""
            $sub = Read-Host "  Pick (a/b/c)"
            switch ($sub) {
                "a" { npm run at-most-once }
                "b" { npm run at-least-once }
                "c" { npm run idempotent }
                default { Write-Host "  Invalid choice" -ForegroundColor Red }
            }
        }
        "8" { npm run offset-reset }
        "9" { Start-Process "http://localhost:8080" }
        "0" {
            Write-Host "  Stopping Kafka..." -ForegroundColor Yellow
            docker compose down -v
            Write-Host "  ✅ Cleaned up!" -ForegroundColor Green
            exit 0
        }
        default { Write-Host "  Invalid choice" -ForegroundColor Red }
    }
}
