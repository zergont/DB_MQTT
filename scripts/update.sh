#!/usr/bin/env bash
#!/usr/bin/env bash
# =============================================================================
# CG DB-Writer — обновление установленного сервиса
#
# Использование:
#   chmod +x scripts/update.sh
#   sudo ./scripts/update.sh
#
# Что делает:
#   1) Копирует актуальные файлы проекта в /opt/db-writer
#   2) Обновляет зависимости из requirements.txt
#   3) Интерактивно сверяет config.example.yml и config.yml
#   4) Применяет SQL схему
#   5) Перезапускает systemd unit
# =============================================================================

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL_DIR="/opt/db-writer"
CONFIG_DIR="/etc/db-writer"
CONFIG_FILE="$CONFIG_DIR/config.yml"
LEGACY_INSTALL_DIR="/home/db-writer"
LEGACY_CONFIG_FILE="$LEGACY_INSTALL_DIR/config.yml"
SERVICE_NAME="cg-db-writer"
SERVICE_USER="cg"

echo "============================================="
echo "  CG DB-Writer — обновление"
echo "============================================="
echo ""

if [ "$(id -u)" -ne 0 ]; then
    echo "ОШИБКА: запустите скрипт через sudo"
    exit 1
fi

if [ ! -t 0 ]; then
    echo "ОШИБКА: update.sh требует интерактивный терминал для merge config.yml"
    exit 1
fi

PYTHON_BIN=""
for candidate in python3.13 python3.12 python3.11 python3.10 python3; do
    if command -v "$candidate" >/dev/null 2>&1; then
        if "$candidate" - <<'PY'
import sys
ok = (sys.version_info.major, sys.version_info.minor) >= (3, 10)
raise SystemExit(0 if ok else 1)
PY
        then
            PYTHON_BIN="$candidate"
            break
        fi
    fi
done

if [ -z "$PYTHON_BIN" ]; then
    echo "ОШИБКА: Python 3.10+ не найден"
    exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "[1/6] Копирование файлов..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$CONFIG_DIR"
rm -rf "$INSTALL_DIR/src" "$INSTALL_DIR/schema" "$INSTALL_DIR/scripts"
cp -r "$REPO_DIR/src" "$INSTALL_DIR/"
cp -r "$REPO_DIR/schema" "$INSTALL_DIR/"
cp -r "$REPO_DIR/scripts" "$INSTALL_DIR/"
cp "$REPO_DIR/requirements.txt" "$INSTALL_DIR/"
cp "$REPO_DIR/config.example.yml" "$INSTALL_DIR/"
echo "  Файлы обновлены"

echo ""
echo "[2/6] Обновление зависимостей..."
cd "$INSTALL_DIR"
if [ ! -d "venv" ]; then
    "$PYTHON_BIN" -m venv venv
    echo "  Создан новый venv в $INSTALL_DIR/venv"
fi
venv/bin/pip install --quiet --upgrade pip
venv/bin/pip install --quiet -r requirements.txt
echo "  Зависимости обновлены"

echo ""
echo "[3/6] Сверка config.yml с новым config.example.yml..."

if [ ! -f "$CONFIG_FILE" ]; then
    if [ -f "$LEGACY_CONFIG_FILE" ]; then
        cp "$LEGACY_CONFIG_FILE" "$CONFIG_FILE"
        echo "  Перенесён старый config.yml из $LEGACY_CONFIG_FILE"
    else
        cp "$INSTALL_DIR/config.example.yml" "$CONFIG_FILE"
        echo "  $CONFIG_FILE отсутствовал, создан из config.example.yml"
    fi
else
    cp "$CONFIG_FILE" "$TMP_DIR/config.yml.backup"
    echo "  Резервная копия config.yml: $TMP_DIR/config.yml.backup"
    export CG_UPDATE_TARGET_CONFIG="$CONFIG_FILE"
    export CG_UPDATE_EXAMPLE_CONFIG="$INSTALL_DIR/config.example.yml"
    export CG_UPDATE_MERGED_CONFIG="$TMP_DIR/config.merged.yml"
    export CG_UPDATE_TTY="/dev/tty"

    "$INSTALL_DIR/venv/bin/python" -c '
from __future__ import annotations

import os
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


target_path = Path(os.environ["CG_UPDATE_TARGET_CONFIG"])
example_path = Path(os.environ["CG_UPDATE_EXAMPLE_CONFIG"])
merged_path = Path(os.environ["CG_UPDATE_MERGED_CONFIG"])
tty_path = Path(os.environ.get("CG_UPDATE_TTY", "/dev/tty"))


try:
    tty_in = tty_path.open("r", encoding="utf-8", errors="ignore")
except OSError as e:
    print(f"ОШИБКА: не удалось открыть {tty_path} для ввода: {e}")
    sys.exit(1)


def load_yaml(path: Path) -> Any:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def prompt_choice(path: str, old_value: Any, new_value: Any) -> Any:
    print()
    print(f"Параметр: {path}")
    print(f"  old: {old_value!r}")
    print(f"  new: {new_value!r}")
    while True:
        print("Выбрать old/new? [old/new]: ", end="", flush=True)
        choice = tty_in.readline()
        if not choice:
            print("\nОШИБКА: stdin терминала закрыт")
            sys.exit(1)
        choice = choice.strip().lower()
        if choice == "old":
            return old_value
        if choice == "new":
            return new_value
        print("Введите 'old' или 'new'.")


def merge(old_cfg: Any, new_cfg: Any, path: str = "") -> Any:
    if isinstance(old_cfg, dict) and isinstance(new_cfg, dict):
        result = deepcopy(old_cfg)
        for key, new_value in new_cfg.items():
            current_path = f"{path}.{key}" if path else key
            if key not in old_cfg:
                print()
                print(f"Новый параметр: {current_path} = {new_value!r}")
                while True:
                    print("Добавить в config.yml? [yes/no]: ", end="", flush=True)
                    choice = tty_in.readline()
                    if not choice:
                        print("\nОШИБКА: stdin терминала закрыт")
                        sys.exit(1)
                    choice = choice.strip().lower()
                    if choice in {"y", "yes", "да"}:
                        result[key] = deepcopy(new_value)
                        break
                    if choice in {"n", "no", "нет"}:
                        break
                    print("Введите 'yes' или 'no'.")
            else:
                result[key] = merge(old_cfg[key], new_value, current_path)
        return result

    if isinstance(old_cfg, list) and isinstance(new_cfg, list):
        return prompt_choice(path or "<root>", old_cfg, new_cfg)

    if old_cfg != new_cfg:
        return prompt_choice(path or "<root>", old_cfg, new_cfg)

    return deepcopy(old_cfg)


old_config = load_yaml(target_path)
new_example = load_yaml(example_path)

if not isinstance(old_config, dict) or not isinstance(new_example, dict):
    print("ОШИБКА: ожидается YAML-словарь в config.yml и config.example.yml")
    sys.exit(1)

merged = merge(old_config, new_example)

with merged_path.open("w", encoding="utf-8") as f:
    yaml.safe_dump(merged, f, allow_unicode=True, sort_keys=False)

tty_in.close()

print()
print(f"Временный merged config сохранён: {merged_path}")
' 

    cp "$TMP_DIR/config.merged.yml" "$CONFIG_FILE"
    echo "  $CONFIG_FILE обновлён после интерактивной сверки"
fi

echo ""
echo "[4/6] Применение SQL схемы..."
if "$INSTALL_DIR/venv/bin/python" "$INSTALL_DIR/scripts/setup_db.py" --config "$CONFIG_FILE" > /tmp/cg-db-writer-update-db.txt 2>&1; then
    echo "  Схема применена"
else
    echo "  ОШИБКА: не удалось применить схему"
    cat /tmp/cg-db-writer-update-db.txt
    rm -f /tmp/cg-db-writer-update-db.txt
    exit 1
fi
rm -f /tmp/cg-db-writer-update-db.txt

echo ""
echo "[5/6] Обновление systemd..."
cp "$REPO_DIR/systemd/cg-db-writer.service" /etc/systemd/system/
cp "$REPO_DIR/systemd/cg-db-writer-cleanup.service" /etc/systemd/system/
cp "$REPO_DIR/systemd/cg-db-writer-cleanup.timer" /etc/systemd/system/
systemctl daemon-reload
if id "$SERVICE_USER" >/dev/null 2>&1; then
    chown -R "$SERVICE_USER":"$SERVICE_USER" "$INSTALL_DIR"
    chown -R "$SERVICE_USER":"$SERVICE_USER" "$CONFIG_DIR"
fi
chmod 750 "$CONFIG_DIR"
chmod 640 "$CONFIG_FILE" || true
systemctl restart "$SERVICE_NAME"
echo "  Сервис перезапущен"

echo ""
echo "[6/6] Проверка..."
sleep 2
if systemctl is-active --quiet "$SERVICE_NAME"; then
    echo "  ✓ $SERVICE_NAME: active (running)"
else
    echo "  ✗ $SERVICE_NAME: не запустился"
    echo "    Проверьте логи: sudo journalctl -u $SERVICE_NAME -n 50"
fi

echo ""
echo "Обновление завершено."
