param(
    [string]$Snap7Root = (Join-Path (Resolve-Path ".").Path "snap7"),
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 102
)

$ErrorActionPreference = "Stop"

$ServerExe = Join-Path $Snap7Root "win64\server.exe"
if (-not (Test-Path -LiteralPath $ServerExe)) {
    $ServerExe = Join-Path $Snap7Root "examples\plain-c\win64\server.exe"
}
if (-not (Test-Path -LiteralPath $ServerExe)) {
    throw "Snap7 server.exe not found under '$Snap7Root'. Expected win64\server.exe or examples\plain-c\win64\server.exe"
}

$TargetDir = Join-Path (Resolve-Path ".").Path "target"
New-Item -ItemType Directory -Force -Path $TargetDir | Out-Null
$PidFile = Join-Path $TargetDir "snap7-sim.pid"
$OutLog = Join-Path $TargetDir "snap7-sim.out.log"
$ErrLog = Join-Path $TargetDir "snap7-sim.err.log"

function Test-PortOpen {
    param([string]$HostName, [int]$PortNumber)
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $iar = $client.BeginConnect($HostName, $PortNumber, $null, $null)
        if (-not $iar.AsyncWaitHandle.WaitOne(300)) { return $false }
        $client.EndConnect($iar)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $client.Close()
    }
}

if (Test-PortOpen -HostName $BindHost -PortNumber $Port) {
    Write-Host "Snap7 simulation already reachable at ${BindHost}:$Port"
    exit 0
}

Write-Host "Starting Snap7 simulation: $ServerExe"
$proc = Start-Process `
    -FilePath $ServerExe `
    -WorkingDirectory (Split-Path -Parent $ServerExe) `
    -WindowStyle Hidden `
    -RedirectStandardOutput $OutLog `
    -RedirectStandardError $ErrLog `
    -PassThru

Set-Content -LiteralPath $PidFile -Value $proc.Id

Start-Sleep -Milliseconds 1000
if ($proc.HasExited) {
    throw "Snap7 simulation exited immediately with code $($proc.ExitCode). stdout=$OutLog stderr=$ErrLog"
}

if (-not (Test-PortOpen -HostName $BindHost -PortNumber $Port)) {
    throw "Snap7 simulation process started (pid=$($proc.Id)) but ${BindHost}:$Port is not reachable. Check $OutLog and $ErrLog"
}

Write-Host "Snap7 simulation running at ${BindHost}:$Port pid=$($proc.Id)"
