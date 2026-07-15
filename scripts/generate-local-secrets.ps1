param([switch]$Force)

$root = Split-Path -Parent $PSScriptRoot
$secretDirectory = Join-Path $root "secrets"
New-Item -ItemType Directory -Path $secretDirectory -Force | Out-Null

function New-HexSecret([int]$byteCount) {
    $bytes = New-Object byte[] $byteCount
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $generator.GetBytes($bytes)
    $generator.Dispose()
    return -join ($bytes | ForEach-Object { $_.ToString("x2") })
}

function Write-Secret([string]$name, [string]$value) {
    $path = Join-Path $secretDirectory $name
    if ($Force -or -not (Test-Path -LiteralPath $path)) {
        [System.IO.File]::WriteAllText($path, $value + [Environment]::NewLine)
    }
}

function Get-OrCreateSecret([string]$name, [int]$byteCount) {
    $path = Join-Path $secretDirectory $name
    if (-not $Force -and (Test-Path -LiteralPath $path)) {
        return (Get-Content -LiteralPath $path -Raw).Trim()
    }

    $value = New-HexSecret $byteCount
    [System.IO.File]::WriteAllText($path, $value + [Environment]::NewLine)
    return $value
}

$postgresPassword = Get-OrCreateSecret "postgres-password.txt" 24
$rabbitPassword = Get-OrCreateSecret "rabbitmq-password.txt" 24
Write-Secret "database-url.txt" "postgres://jobs:$postgresPassword@postgres:5432/jobs?sslmode=disable"
Write-Secret "rabbitmq.conf" "default_user = jobs`ndefault_pass = $rabbitPassword"
Write-Secret "rabbitmq-url.txt" "amqp://jobs:$rabbitPassword@rabbitmq:5672/"
$null = Get-OrCreateSecret "grpc-token.txt" 32

$certificate = Join-Path $secretDirectory "grpc-server.crt"
$privateKey = Join-Path $secretDirectory "grpc-server.key"
if ($Force -or -not (Test-Path -LiteralPath $certificate) -or -not (Test-Path -LiteralPath $privateKey)) {
    $openssl = Get-Command openssl -ErrorAction Stop
    & $openssl.Source req -x509 -newkey rsa:2048 -sha256 -nodes -days 365 `
        -subj "/CN=api" `
        -addext "subjectAltName=DNS:api,DNS:localhost,IP:127.0.0.1" `
        -keyout $privateKey -out $certificate
    if ($LASTEXITCODE -ne 0) {
        throw "OpenSSL could not generate the local gRPC certificate"
    }
}

Write-Output "Local secret files are ready in $secretDirectory"
