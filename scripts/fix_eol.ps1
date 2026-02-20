\
# Fix line endings in repo to LF (Unix) to avoid systemd and /usr/bin/env^M issues
# Run from repo root:
#   powershell -ExecutionPolicy Bypass -File .\scripts\fix_eol.ps1
#
# What it does:
# - Converts CRLF and CR-only to LF for text files used by Linux deployment
# - Ensures the file ends with a newline

$ErrorActionPreference = "Stop"

$extensions = @(
  ".py",".sh",".yml",".yaml",".sql",".md",".service",".timer",".ps1",
  ".gitattributes",".gitignore",".editorconfig"
)

Write-Host "== CG DB_MQTT: fixing EOL to LF ==" -ForegroundColor Cyan
Write-Host "Repo: $(Get-Location)"
Write-Host ""

$changed = 0
$scanned = 0

function ShouldFix($path) {
  $name = [System.IO.Path]::GetFileName($path)
  if ($name -eq ".gitattributes" -or $name -eq ".gitignore" -or $name -eq ".editorconfig") { return $true }
  $ext = [System.IO.Path]::GetExtension($path)
  return $extensions -contains $ext
}

Get-ChildItem -Recurse -File | ForEach-Object {
  $p = $_.FullName
  if (-not (ShouldFix $p)) { return }
  $scanned++

  $bytes = [System.IO.File]::ReadAllBytes($p)
  if ($bytes.Length -eq 0) { return }

  # Read as UTF-8 (without BOM)
  $text = [System.Text.Encoding]::UTF8.GetString($bytes)

  # Replace CRLF -> LF and CR -> LF
  $fixed = $text -replace "`r`n", "`n"
  $fixed = $fixed -replace "`r", "`n"

  # Ensure final newline
  if (-not $fixed.EndsWith("`n")) { $fixed = $fixed + "`n" }

  if ($fixed -ne $text) {
    [System.IO.File]::WriteAllText($p, $fixed, (New-Object System.Text.UTF8Encoding($false)))
    $changed++
    Write-Host "FIXED: $($_.FullName)"
  }
}

Write-Host ""
Write-Host "Scanned: $scanned files"
Write-Host "Changed: $changed files"
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1) In Visual Studio: Git -> Changes -> Commit All"
Write-Host "  2) Push to GitHub"
Write-Host "  3) Re-check raw files on GitHub (they should have normal line breaks)"
