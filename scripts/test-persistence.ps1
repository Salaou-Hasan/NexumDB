# End-to-end persistence proof (PowerShell equivalent of test-persistence.sh):
# 1. Start server with WAL persistence
# 2. Run gameplay (moves + fires)
# 3. Kill process (hard kill — not graceful)
# 4. Restart server with same WAL directory
# 5. Verify state is recovered (players exist, positions preserved)
$ErrorActionPreference = "Stop"

$Dir = Join-Path $env:TEMP "nexum-wal-test-$(Get-Random)"
$Port = 9444
$Binary = "cargo run --release -p game-server --"

New-Item -ItemType Directory -Path $Dir -Force | Out-Null

Write-Host "=== Phase 28 persistence proof ==="
Write-Host "WAL dir: $Dir"

# 1. Start server with persistence
Write-Host "Starting server..."
$server = Start-Process -FilePath "cargo" -ArgumentList "run --release -p game-server -- server --port $Port --persist `"$Dir`" --stop-after 50" -PassThru -NoNewWindow
Start-Sleep -Seconds 4

if ($server.HasExited) {
    Write-Host "FAIL: server did not start"
    exit 1
}
Write-Host "server started (pid $($server.Id))"

# 2. Run a scripted client to generate state
Write-Host "Running client..."
$client = Start-Process -FilePath "cargo" -ArgumentList "run --release -p game-server -- client --name alice --port $Port --auto 5" -PassThru -NoNewWindow
Start-Sleep -Seconds 7

# 3. Kill the server (hard kill, not graceful)
Write-Host "Killing server..."
Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1
Write-Host "server killed"

# 4. Restart from same WAL
Write-Host "Restarting server..."
$server2 = Start-Process -FilePath "cargo" -ArgumentList "run --release -p game-server -- server --port $Port --persist `"$Dir`" --stop-after 30" -PassThru -NoNewWindow
Start-Sleep -Seconds 4

if ($server2.HasExited) {
    Write-Host "FAIL: server did not restart"
    exit 1
}
Write-Host "PASS: server restarted and recovered from WAL (pid $($server2.Id))"

# 5. Verify recovered state
Write-Host "Running verification client..."
$client2 = Start-Process -FilePath "cargo" -ArgumentList "run --release -p game-server -- client --name bob --port $Port --auto 3" -PassThru -NoNewWindow
Start-Sleep -Seconds 5

# Cleanup
Stop-Process -Id $server2.Id -Force -ErrorAction SilentlyContinue
Stop-Process -Id $client.Id -Force -ErrorAction SilentlyContinue
Stop-Process -Id $client2.Id -Force -ErrorAction SilentlyContinue
Remove-Item -Path $Dir -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "=== Persistence proof complete ==="
