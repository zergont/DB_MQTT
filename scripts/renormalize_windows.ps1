# CG DB_MQTT: normalize line endings in the repository (run in PowerShell)
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\scripts\renormalize_windows.ps1

Write-Host "== CG DB_MQTT: normalize line endings to LF =="

# Ensure git does not auto-convert to CRLF in working tree
git config core.autocrlf false | Out-Null

# Renormalize according to .gitattributes
git add --renormalize . | Out-Null

Write-Host ""
Write-Host "Review changes:" -ForegroundColor Cyan
Write-Host "  git status"
Write-Host "  git diff --stat"
Write-Host ""
Write-Host "If everything looks good, commit:" -ForegroundColor Cyan
Write-Host '  git commit -m "Normalize EOL to LF"'
