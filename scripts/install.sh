#!/usr/bin/env bash
# =============================================================================
# CG DB-Writer — установка на Ubuntu (запускать из корня репозитория)
#
# Использование:
#   chmod +x scripts/install.sh
#   sudo ./scripts/install.sh
#
# Что делает:
#   1) Проверяет Python 3.10+ и PostgreSQL
#   2) Создаёт venv и ставит зависимости
#   3) Копирует config.example.yml → config.yml (если нет)
#   4) Устанавливает systemd unit-файлы
#   5) Подсказывает следующие шаги
# =============================================================================

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL_DIR="/home/db-writer"
SERVICE_USER="cg"

echo "============================================="
echo "  CG DB-Writer — установка"
echo "============================================="
echo ""

# --- 1) Проверки ---
echo "[1/5] Проверка зависимостей..."

# Требуем Python 3.10+ (можно как python3.12, python3.13, так и python3 нужной версии)
PYTHON_BIN=""
for candidate in python3.13 python3.12 python3.11 python3.10 python3; do
    if command -v "$candidate" &> /dev/null; then
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
    echo "  ОШИБКА: Python 3.10+ не найден"
    python3 --version 2>/dev/null || true
    echo "  Установите Python 3.10+ (и пакет venv) и повторите установку."
    exit 1
fi

echo "  Python: $($PYTHON_BIN --version)"

if ! command -v psql &> /dev/null; then
    echo "  ПРЕДУПРЕЖДЕНИЕ: psql не найден (PostgreSQL client)"
    echo "  Установите: sudo apt install postgresql-client"
fi

# --- 2) Установка файлов ---
echo ""
echo "[2/5] Копирование файлов в $INSTALL_DIR..."

if [ "$REPO_DIR" != "$INSTALL_DIR" ]; then
    mkdir -p "$INSTALL_DIR"
    cp -r "$REPO_DIR/src" "$INSTALL_DIR/"
    cp -r "$REPO_DIR/schema" "$INSTALL_DIR/"
    cp -r "$REPO_DIR/scripts" "$INSTALL_DIR/"
    cp "$REPO_DIR/requirements.txt" "$INSTALL_DIR/"
    cp "$REPO_DIR/config.example.yml" "$INSTALL_DIR/"
    echo "  Скопировано в $INSTALL_DIR"
else
    echo "  Уже в $INSTALL_DIR, пропускаю"
fi

# --- 3) Virtual environment ---
echo ""
echo "[3/5] Создание venv и установка зависимостей..."

PYTHON_BIN="${PYTHON_BIN:-python3}"


cd "$INSTALL_DIR"
if [ ! -d "venv" ]; then
    $PYTHON_BIN -m venv venv
    echo "  venv создан"
else
    echo "  venv уже существует"
fi

venv/bin/pip install --quiet --upgrade pip
venv/bin/pip install --quiet -r requirements.txt
echo "  Зависимости установлены"

# --- 4) Config ---
echo ""
echo "[4/5] Конфигурация..."

if [ ! -f "$INSTALL_DIR/config.yml" ]; then
    cp "$INSTALL_DIR/config.example.yml" "$INSTALL_DIR/config.yml"
    echo "  Создан config.yml (ЗАПОЛНИТЕ секреты: mqtt, postgres)"
    echo "  Редактировать: nano $INSTALL_DIR/config.yml"
else
    echo "  config.yml уже существует"
fi

# --- 5) Системный пользователь и systemd ---
echo ""
echo "[5/5] Systemd..."

if ! id "$SERVICE_USER" &> /dev/null; then
    useradd -r -s /usr/sbin/nologin "$SERVICE_USER"
    echo "  Создан пользователь: $SERVICE_USER"
fi

chown -R "$SERVICE_USER":"$SERVICE_USER" "$INSTALL_DIR"

cp "$REPO_DIR/systemd/cg-db-writer.service" /etc/systemd/system/
cp "$REPO_DIR/systemd/cg-db-writer-cleanup.service" /etc/systemd/system/
cp "$REPO_DIR/systemd/cg-db-writer-cleanup.timer" /etc/systemd/system/
systemctl daemon-reload
echo "  Systemd unit-файлы установлены"

# --- Итого ---
echo ""
echo "============================================="
echo "  Установка завершена!"
echo "============================================="
echo ""
echo "Следующие шаги:"
echo ""
echo "  1) Заполните config.yml:"
echo "     nano $INSTALL_DIR/config.yml"
echo ""
echo "  2) Создайте БД PostgreSQL (если ещё нет):"
echo "     sudo -u postgres psql"
echo "     CREATE USER cg_writer WITH PASSWORD 'your_password';"
echo "     CREATE DATABASE cg_telemetry OWNER cg_writer;"
echo "     \\q"
echo ""
echo "  3) Примените схему:"
echo "     cd $INSTALL_DIR"
echo "     venv/bin/python scripts/setup_db.py --config config.yml"
echo ""
echo "  4) Проверьте подключения:"
echo "     venv/bin/python scripts/check_health.py --config config.yml"
echo ""
echo "  5) Запустите сервис:"
echo "     sudo systemctl enable --now cg-db-writer"
echo ""
echo "  6) (Опционально) Вынесите очистку в systemd timer:"
echo "     sudo systemctl enable --now cg-db-writer-cleanup.timer"
echo ""
echo "  7) Проверьте логи:"
echo "     sudo journalctl -u cg-db-writer -f"
echo ""
