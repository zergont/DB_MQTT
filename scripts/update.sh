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
#   1) Копирует актуальные файлы проекта в /home/db-writer
#   2) Обновляет зависимости из requirements.txt
#   3) Интерактивно сверяет config.example.yml и config.yml
#   4) Применяет SQL схему
#   5) Перезапускает systemd unit
# =============================================================================

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL_DIR="/home/db-writer"
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

if [ ! -d "$INSTALL_DIR" ]; then
    echo "ОШИБКА: каталог установки не найден: $INSTALL_DIR"
    echo "Сначала выполните установку: sudo ./scripts/install.sh"
    exit 1
fi

if [ ! -f "$INSTALL_DIR/venv/bin/python" ]; then
    echo "ОШИБКА: venv не найден в $INSTALL_DIR/venv"
    exit 1
fi

if [ ! -t 0 ]; then
    echo "ОШИБКА: update.sh требует интерактивный терминал для merge config.yml"
    exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "[1/6] Копирование файлов..."
mkdir -p "$INSTALL_DIR"
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
venv/bin/pip install --quiet --upgrade pip
venv/bin/pip install --quiet -r requirements.txt
echo "  Зависимости обновлены"

echo ""
echo "[3/6] Сверка config.yml с новым config.example.yml..."

if [ ! -f "$INSTALL_DIR/config.yml" ]; then
    cp "$INSTALL_DIR/config.example.yml" "$INSTALL_DIR/config.yml"
    echo "  config.yml отсутствовал, создан из config.example.yml"
else
    cp "$INSTALL_DIR/config.yml" "$TMP_DIR/config.yml.backup"
    echo "  Резервная копия config.yml: $TMP_DIR/config.yml.backup"
    export CG_UPDATE_TARGET_CONFIG="$INSTALL_DIR/config.yml"
    export CG_UPDATE_EXAMPLE_CONFIG="$INSTALL_DIR/config.example.yml"
    export CG_UPDATE_MERGED_CONFIG="$TMP_DIR/config.merged.yml"

    "$INSTALL_DIR/venv/bin/python" <<'PY'
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
        choice = input("Выбрать old/new? [old/new]: ").strip().lower()
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
                    choice = input("Добавить в config.yml? [yes/no]: ").strip().lower()
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

print()
print(f"Временный merged config сохранён: {merged_path}")
PY

    cp "$TMP_DIR/config.merged.yml" "$INSTALL_DIR/config.yml"
    echo "  config.yml обновлён после интерактивной сверки"
fi

echo ""
echo "[4/6] Применение SQL схемы..."
if "$INSTALL_DIR/venv/bin/python" "$INSTALL_DIR/scripts/setup_db.py" --config "$INSTALL_DIR/config.yml" > /tmp/cg-db-writer-update-db.txt 2>&1; then
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
fi
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
