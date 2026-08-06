param(
    [Parameter(Mandatory = $false)]
    [string]$ChangedPath = ""
)

# Skip for docs-only edits by default.
if ($ChangedPath -and ($ChangedPath -like "*.md")) {
    exit 0
}

# Run repository default test command as v1 safety net.
python -m unittest discover -s tests -v
if ($LASTEXITCODE -ne 0) {
    Write-Error "Post-tool validation failed."
    exit $LASTEXITCODE
}

exit 0
