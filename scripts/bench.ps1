<#
.SYNOPSIS
    caddy-ws-tunnel-plugin M6 压测基准 runner（Windows / PowerShell 7）。

.DESCRIPTION
    本脚本只做三件事：把本机 Go 放进 PATH、选择矩阵、调用 go test 跑固定窗口 benchmark，
    并把原始输出（含 M6ENV / M6CSV 行）tee 到日志文件，便于复现与汇总进报告。

    矩阵入口在 test/bench_test.go：
      BenchmarkTunnelCapacity  channels × max_streams_per_conn × 并发（512B 小响应）
      BenchmarkTunnelHOL       单通道 vs 多通道 + 64KB 大响应混合 + trickle 慢流

    注意：这两个 benchmark 是"固定时长窗口"语义，必须配合 -benchtime=1x 运行；
    每次场景内部自行计时 3s（可用 M6_WINDOW 覆盖），b.N 被忽略。

.PARAMETER Mode
    full      正式矩阵：3 通道数 × 3 流上限 × 4 并发 + HOL 全套（约 4 分钟）
    capacity  只跑容量矩阵
    hol       只跑 HOL 观测
    smoke     冒烟：-short（缩小矩阵、1s 窗口），约 20s

.PARAMETER OutFile
    输出日志路径，默认 docs/../.bench-logs/m6-<mode>-<timestamp>.log（已被 .gitignore 忽略前的本地目录）。

.EXAMPLE
    pwsh -File scripts/bench.ps1 -Mode full
    pwsh -File scripts/bench.ps1 -Mode smoke
#>
[CmdletBinding()]
param(
    [ValidateSet('full', 'capacity', 'hol', 'smoke')]
    [string]$Mode = 'full',

    [string]$OutFile = '',

    [string]$Window = '',
    [string]$Warmup = '',

    # 本机 Go 安装目录；默认取任务说明里的路径。
    [string]$GoBin = "$env:USERPROFILE\sdk\go1.27.4\bin"
)

$ErrorActionPreference = 'Stop'

# 1) Go 进 PATH（本机没有全局 go）
if (Test-Path (Join-Path $GoBin 'go.exe')) {
    $env:PATH = "$GoBin;$env:PATH"
} elseif (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw "找不到 go：$GoBin\go.exe 不存在，且 PATH 里没有 go。请用 -GoBin 指定。"
}

$repo = Split-Path -Parent $PSScriptRoot
Push-Location $repo
try {
    # 2) 依赖代理提示（本机默认 proxy.golang.org 不可达，应已全局设置 goproxy.cn）
    $goproxy = (& go env GOPROXY) 2>$null
    if ($goproxy -notmatch 'goproxy\.cn|goproxy\.io|direct') {
        Write-Warning "GOPROXY=$goproxy；若首次拉依赖很慢，可先执行: go env -w GOPROXY=https://goproxy.cn,direct"
    }

    # 3) 选择矩阵
    $bench = 'Benchmark'
    $extra = @()
    switch ($Mode) {
        'full' { $bench = 'Benchmark' }
        'capacity' { $bench = 'BenchmarkTunnelCapacity' }
        'hol' { $bench = 'BenchmarkTunnelHOL' }
        'smoke' { $bench = 'Benchmark'; $extra += '-short' }
    }

    if ($Mode -eq 'full') {
        Remove-Item Env:M6_CHANNELS, Env:M6_STREAMS, Env:M6_CONC -ErrorAction SilentlyContinue
    }
    if ($Window) { $env:M6_WINDOW = $Window } else { Remove-Item Env:M6_WINDOW -ErrorAction SilentlyContinue }
    if ($Warmup) { $env:M6_WARMUP = $Warmup } else { Remove-Item Env:M6_WARMUP -ErrorAction SilentlyContinue }

    if (-not $OutFile) {
        # 默认写到系统临时目录，避免往仓库里塞运行产物。
        $dir = Join-Path ([IO.Path]::GetTempPath()) 'caddy-ws-tunnel-m6'
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        $OutFile = Join-Path $dir ("m6-{0}-{1}.log" -f $Mode, (Get-Date -Format 'yyyyMMdd-HHmmss'))
    }

    $cmdArgs = @('test', './test', '-run', '^$', '-bench', $bench, '-benchtime=1x') + $extra
    Write-Host ("[bench] go " + ($cmdArgs -join ' ')) -ForegroundColor Cyan
    Write-Host ("[bench] log -> $OutFile") -ForegroundColor Cyan

    # 4) 跑并 tee；保留 go test 的退出码
    & go @cmdArgs 2>&1 | Tee-Object -FilePath $OutFile
    $code = $LASTEXITCODE

    # 5) 摘要：CSV 行数 + 退出码
    $csv = Get-Content $OutFile | Where-Object { $_ -like 'M6CSV,*' }
    Write-Host ("[bench] M6CSV rows = {0}, exit = {1}" -f ($csv.Count - 1), $code) -ForegroundColor Cyan
    exit $code
}
finally {
    Pop-Location
}
