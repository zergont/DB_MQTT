# EOL fix scripts for DB_MQTT

Почему это нужно:
- На GitHub raw часть файлов отображается как "1 строка" — это признак CR-only концов строк.
- На Ubuntu это ломает systemd и shebang (/usr/bin/env^M / пути с \r в конце).

Как применить (Windows / VS2022):
1) Скопируйте папку `scripts/` в корень репозитория (или распакуйте архив поверх).
2) Откройте PowerShell в корне репо и выполните:
   powershell -ExecutionPolicy Bypass -File .\scripts\fix_eol.ps1
   (или) python .\scripts\fix_eol.py
3) В Visual Studio: Git -> Changes -> Commit All -> Push
4) Проверьте на GitHub raw, например:
   - systemd/cg-db-writer.service
   - scripts/check_health.py
   - .gitattributes
   Должны быть нормальные переносы строк.
