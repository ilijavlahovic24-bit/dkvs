$jobs = @()

$jobs += Start-Process go -ArgumentList "run main.go -db-location=belgrade.db -http-addr=127.0.0.1:8080 -config-file=sharding.toml -shard=Belgrade" -PassThru -NoNewWindow
$jobs += Start-Process go -ArgumentList "run main.go -db-location=nis.db -http-addr=127.0.0.1:8081 -config-file=sharding.toml -shard=Nis" -PassThru -NoNewWindow
$jobs += Start-Process go -ArgumentList "run main.go -db-location=kragujevac.db -http-addr=127.0.0.1:8082 -config-file=sharding.toml -shard=Kragujevac" -PassThru -NoNewWindow

Write-Host "DKVS running. Press Ctrl+C to stop..."

try {
    Wait-Process -Id ($jobs | ForEach-Object { $_.Id })
} finally {
    $jobs | ForEach-Object { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue }
}