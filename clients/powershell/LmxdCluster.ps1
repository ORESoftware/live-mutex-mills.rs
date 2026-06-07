# LmxdCluster.ps1 — PowerShell harness/client for the live-mutex-mills `lmxd` daemon.
#
# The Windows-shell companion to clients/shell. live-mutex-mills is leaderless —
# there is no central server, so the client surface is the daemon's own
# stdin/stdout protocol (README "Then type commands on stdin"):
#
#     stdin  : acquire <lock> | release <lock> | quit
#     stdout : ACQUIRED <lock> fence=<n> | LOST <lock> | # <info>
#
# This launches a local N-node lmxd cluster over loopback (each node a
# System.Diagnostics.Process with redirected stdin/stdout) and drives it through
# that interface. Requires the `lmxd` binary built from this repo with cargo.
#
# Dot-source this file and use [LmxdCluster]::new(); see smoke.ps1 for usage.

Set-StrictMode -Version Latest

class LmxdCluster {
    [string] $Bin
    [int] $BasePort = 9300
    [string] $Codec = 'text'
    [int] $WaitSeconds = 20
    [System.Collections.ArrayList] $Nodes = [System.Collections.ArrayList]::new()
    [System.Collections.ArrayList] $Buffers = [System.Collections.ArrayList]::new()
    [double] $LastFence = 0

    LmxdCluster() { $this.Bin = [LmxdCluster]::ResolveBin() }

    static [string] ResolveBin() {
        $repo = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        $exe = if ($IsWindows) { 'lmxd.exe' } else { 'lmxd' }
        foreach ($p in @("target/release/$exe", "target/debug/$exe")) {
            $full = Join-Path $repo $p
            if (Test-Path $full) { return $full }
        }
        Write-Host '[lmx] building lmxd (cargo build --release --bin lmxd) ...'
        Push-Location $repo
        try { & cargo build --release --bin lmxd | Out-Host } finally { Pop-Location }
        return (Join-Path $repo "target/release/$exe")
    }

    [void] Start([int] $n) {
        $addrs = 0..($n - 1) | ForEach-Object { "127.0.0.1:$($this.BasePort + $_)" }
        for ($i = 0; $i -lt $n; $i++) {
            $psi = [System.Diagnostics.ProcessStartInfo]::new()
            $psi.FileName = $this.Bin
            $psi.RedirectStandardInput = $true
            $psi.RedirectStandardOutput = $true
            $psi.RedirectStandardError = $true
            $psi.UseShellExecute = $false
            [void]$psi.ArgumentList.Add('--codec'); [void]$psi.ArgumentList.Add($this.Codec)
            [void]$psi.ArgumentList.Add("$i")
            foreach ($a in $addrs) { [void]$psi.ArgumentList.Add($a) }

            $sb = [System.Text.StringBuilder]::new()
            $p = [System.Diagnostics.Process]::new()
            $p.StartInfo = $psi
            $handler = {
                param($s, $e)
                if ($null -ne $e.Data) { [void]$Event.MessageData.AppendLine($e.Data) }
            }
            Register-ObjectEvent -InputObject $p -EventName OutputDataReceived -Action $handler -MessageData $sb | Out-Null
            [void]$p.Start()
            $p.BeginOutputReadLine()
            [void]$this.Nodes.Add($p)
            [void]$this.Buffers.Add($sb)
        }
        Start-Sleep -Seconds 2   # let the mesh dial up
    }

    [void] Send([int] $id, [string] $command) {
        $this.Nodes[$id].StandardInput.WriteLine($command)
        $this.Nodes[$id].StandardInput.Flush()
    }

    [string] NodeLog([int] $id) { return $this.Buffers[$id].ToString() }

    # Drives acquire on a node; sets $this.LastFence. Returns the fence.
    [double] Acquire([int] $id, [string] $lock) {
        $this.Send($id, "acquire $lock")
        $deadline = (Get-Date).AddSeconds($this.WaitSeconds)
        $rx = [regex]"ACQUIRED $([regex]::Escape($lock)) fence=(\d+)"
        while ((Get-Date) -lt $deadline) {
            $m = $rx.Match($this.Buffers[$id].ToString())
            if ($m.Success) { $this.LastFence = [double]$m.Groups[1].Value; return $this.LastFence }
            Start-Sleep -Milliseconds 100
        }
        throw "timed out waiting for ACQUIRED $lock on node $id"
    }

    [void] Release([int] $id, [string] $lock) { $this.Send($id, "release $lock") }

    [void] Stop() {
        foreach ($p in $this.Nodes) {
            try { if (-not $p.HasExited) { $p.Kill() } } catch { }
        }
        $this.Nodes.Clear(); $this.Buffers.Clear()
        Get-EventSubscriber | Where-Object { $_.SourceObject -is [System.Diagnostics.Process] } |
            ForEach-Object { Unregister-Event -SubscriptionId $_.SubscriptionId -ErrorAction SilentlyContinue }
    }
}
