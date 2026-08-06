param(
    [Parameter(Mandatory = $false)]
    [string]$ToolCommand = ""
)

if (-not $ToolCommand) {
    exit 0
}

$blockedPatterns = @(
    "git reset --hard",
    "git checkout --",
    "git clean -fd",
    "git clean -xfd"
)

foreach ($pattern in $blockedPatterns) {
    if ($ToolCommand -like "*$pattern*") {
        Write-Error "Blocked command pattern detected: $pattern"
        exit 1
    }
}

exit 0
