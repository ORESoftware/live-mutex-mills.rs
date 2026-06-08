# End-to-end smoke test for the live-mutex-mills PowerShell harness, mirroring
# clients/shell/smoke.sh.
#
#   pwsh ./smoke.ps1
#
# Launches a local N-node lmxd cluster over loopback, acquires a lock on one
# node (observing its fence), releases it, then re-acquires from a different
# node and asserts the fence strictly increased — the monotonic per-lock fencing
# the protocol guarantees across handoffs.
#
# Tunables (env): LMX_NODES, LMX_BASE_PORT, LMX_CODEC (text|json|msgpack).

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'LmxdCluster.ps1')

$n = if ($env:LMX_NODES) { [int]$env:LMX_NODES } else { 3 }

$cluster = [LmxdCluster]::new()
if ($env:LMX_BASE_PORT) { $cluster.BasePort = [int]$env:LMX_BASE_PORT }
if ($env:LMX_CODEC) { $cluster.Codec = $env:LMX_CODEC }

Write-Host "[smoke-powershell] starting ${n}-node lmxd cluster (codec=$($cluster.Codec))"
try {
    $cluster.Start($n)

    $first = $cluster.Acquire(0, 'smoke-lock')
    Write-Host "[smoke-powershell] node 0 ACQUIRED smoke-lock fence=$first"
    $cluster.Release(0, 'smoke-lock')
    Write-Host '[smoke-powershell] node 0 released smoke-lock'

    $target = if ($n -gt 1) { 1 } else { 0 }
    $second = $cluster.Acquire($target, 'smoke-lock')
    Write-Host "[smoke-powershell] node $target ACQUIRED smoke-lock fence=$second"
    $cluster.Release($target, 'smoke-lock')

    if (-not ($second -gt $first)) {
        throw "fence must strictly increase across handoff ($first -> $second)"
    }
    Write-Host "[smoke-powershell] fence strictly increased across handoff ($first -> $second)"
    Write-Host '[smoke-powershell] OK'
}
finally {
    $cluster.Stop()
}
