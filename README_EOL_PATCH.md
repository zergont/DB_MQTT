# DB_MQTT — EOL patch (LF)

Зачем:
- На Ubuntu скрипты и unit-файлы часто ломаются с ошибкой вида `/usr/bin/env^M: bad interpreter`,
  если в репозитории появились CR / CRLF окончания строк.
- Этот патч **форсирует LF** через `.gitattributes` и добавляет скрипты для нормализации.

Как применить:
1) Распакуйте архив поверх репозитория (с заменой файлов):
   - `.gitattributes`
   - `.editorconfig` (опционально, но полезно)
   - `.gitignore`
   - `scripts/renormalize_linux.sh`
   - `scripts/renormalize_windows.ps1`
2) Запустите нормализацию:
   - Linux/Ubuntu: `bash scripts/renormalize_linux.sh`
   - Windows (PowerShell): `powershell -ExecutionPolicy Bypass -File .\scripts\renormalize_windows.ps1`
3) Проверьте: `git diff --stat`
4) Закоммитьте: `git commit -m "Normalize EOL to LF"`

После этого: клонируйте на Ubuntu и запускайте установку / сервисы.
