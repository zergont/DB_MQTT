#!/usr/bin/env bash
# =============================================================================
# CG DB-Writer — установка на Ubuntu (запускать из корня репозитория)
#
# Использование:
#   chmod +x scripts/install.sh
#   sudo ./scripts/install.sh
#
# Что делает:
#   1) Проверяет Python 3.10+
#   2) Устанавливает PostgreSQL (если нет) и создаёт БД/пользователя
#   3) Создаёт venv и ставит зависимости
#   4) Копирует config.example.yml → /etc/db-writer/config.yml (если нет)
#   5) Устанавливает systemd unit-файлы
# =============================================================================

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL_DIR="/opt/db-writer"
CONFIG_DIR="/etc/db-writer"
CONFIG_FILE="$CONFIG_DIR/config.yml"
SERVICE_USER="cg"

echo "============================================="
echo "  CG DB-Writer — установка"
echo "============================================="
echo ""

# --- 1) Проверки ---
echo "[1/6] Проверка зависимостей..."

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
    echo "  PostgreSQL не найден — устанавливаю..."
    apt-get update -qq
    apt-get install -y -qq postgresql postgresql-client > /dev/null
    systemctl enable --now postgresql
    echo "  PostgreSQL установлен и запущен"
else
    echo "  PostgreSQL: $(psql --version | head -1)"
    # Убедимся что сервер запущен
    if ! systemctl is-active --quiet postgresql 2>/dev/null; then
        systemctl start postgresql
        echo "  PostgreSQL сервер запущен"
    fi
fi

# --- 1b) Создание БД и пользователя (если ещё нет) ---
PG_USER="cg_writer"
PG_DB="cg_telemetry"
PG_PASS="cg_writer"

# Проверяем существует ли пользователь
if sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='$PG_USER'" 2>/dev/null | grep -q 1; then
    echo "  PG пользователь '$PG_USER' уже существует"
else
    sudo -u postgres psql -c "CREATE USER $PG_USER WITH PASSWORD '$PG_PASS';" > /dev/null 2>&1
    echo "  PG пользователь '$PG_USER' создан (пароль: $PG_PASS)"
fi

# Проверяем существует ли БД
if sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='$PG_DB'" 2>/dev/null | grep -q 1; then
    echo "  БД '$PG_DB' уже существует"
else
    sudo -u postgres psql -c "CREATE DATABASE $PG_DB OWNER $PG_USER;" > /dev/null 2>&1
    echo "  БД '$PG_DB' создана (owner: $PG_USER)"
fi

# --- 2) Установка файлов ---
echo ""
echo "[2/6] Копирование файлов в $INSTALL_DIR..."

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

mkdir -p "$CONFIG_DIR"

# --- 3) Virtual environment ---
echo ""
echo "[3/6] Создание venv и установка зависимостей..."

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
echo "[4/6] Конфигурация..."

if [ ! -f "$CONFIG_FILE" ]; then
    cp "$INSTALL_DIR/config.example.yml" "$CONFIG_FILE"
    echo "  Создан $CONFIG_FILE (postgres: cg_writer/cg_writer)"
    echo "  MQTT секция — заполните при необходимости: nano $CONFIG_FILE"
else
    echo "  $CONFIG_FILE уже существует"
fi

# Применяем SQL схему автоматически
echo "  Применяю SQL схему..."
if "$INSTALL_DIR/venv/bin/python" "$INSTALL_DIR/scripts/setup_db.py" --config "$CONFIG_FILE" > /dev/null 2>&1; then
    echo "  Схема применена"
else
    echo "  ⚠ Не удалось применить схему (проверьте $CONFIG_FILE и запустите вручную)"
    echo "    cd $INSTALL_DIR && venv/bin/python scripts/setup_db.py --config $CONFIG_FILE"
fi

# --- 5) Системный пользователь и systemd ---
echo ""
echo "[5/6] Systemd..."

if ! id "$SERVICE_USER" &> /dev/null; then
    useradd -r -s /usr/sbin/nologin "$SERVICE_USER"
    echo "  Создан пользователь: $SERVICE_USER"
fi

chown -R "$SERVICE_USER":"$SERVICE_USER" "$INSTALL_DIR"
chown -R "$SERVICE_USER":"$SERVICE_USER" "$CONFIG_DIR"
chmod 750 "$CONFIG_DIR"
chmod 640 "$CONFIG_FILE" || true

cp "$REPO_DIR/systemd/cg-db-writer.service" /etc/systemd/system/
cp "$REPO_DIR/systemd/cg-db-writer-cleanup.service" /etc/systemd/system/
cp "$REPO_DIR/systemd/cg-db-writer-cleanup.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable cg-db-writer
echo "  Systemd unit-файлы установлены, автозапуск включён"

# Запускаем / перезапускаем сервис
systemctl restart cg-db-writer
echo "  Сервис cg-db-writer запущен"

# --- 6) Проверка ---
echo ""
echo "[6/6] Проверка..."
sleep 2

if systemctl is-active --quiet cg-db-writer; then
    echo "  ✓ cg-db-writer: active (running)"
else
    echo "  ✗ cg-db-writer: не запустился"
    echo "    Проверьте логи: sudo journalctl -u cg-db-writer -n 20"
fi

# Health check
if "$INSTALL_DIR/venv/bin/python" "$INSTALL_DIR/scripts/check_health.py" --config "$CONFIG_FILE" > /tmp/cg-health-check.txt 2>&1; then
    echo "  ✓ PostgreSQL: OK"
    echo "  ✓ MQTT: OK"
else
    echo "  ⚠ Health check — есть проблемы:"
    cat /tmp/cg-health-check.txt | grep -E 'ОШИБКА|OK' | sed 's/^/    /'
fi
rm -f /tmp/cg-health-check.txt

# --- Итого ---
echo ""
echo "============================================="
echo "  Установка завершена!"
echo "============================================="
echo ""
echo "  Код:     $INSTALL_DIR"
echo "  Конфиг:  $CONFIG_FILE"
echo "  Логи:    sudo journalctl -u cg-db-writer -f"
echo "  Статус:  sudo systemctl status cg-db-writer"
echo ""
