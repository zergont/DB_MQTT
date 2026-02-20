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
#   4) Копирует config.example.yml → config.yml (если нет)
#   5) Устанавливает systemd unit-файлы
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
PG_PASS_NEW=""

# Проверяем существует ли пользователь
if sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='$PG_USER'" 2>/dev/null | grep -q 1; then
    echo "  PG пользователь '$PG_USER' уже существует"
else
    PG_PASS_NEW="cg_$(head -c 12 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 12)"
    sudo -u postgres psql -c "CREATE USER $PG_USER WITH PASSWORD '$PG_PASS_NEW';" > /dev/null 2>&1
    echo "  PG пользователь '$PG_USER' создан (пароль: $PG_PASS_NEW)"
    echo "  ⚠ ЗАПОМНИТЕ ПАРОЛЬ — он понадобится для config.yml"
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
    # Автозаполнение postgres секции если пользователь был только что создан
    if [ -n "${PG_PASS_NEW:-}" ]; then
        # Заменяем только в postgres секции (вторые вхождения user/password)
        sed -i '/^postgres:/,/^[^ ]/ {
            s|^  user: "".*|  user: "'"$PG_USER"'"|
            s|^  password: "".*|  password: "'"$PG_PASS_NEW"'"|
        }' "$INSTALL_DIR/config.yml"
        echo "  Создан config.yml (postgres секция заполнена автоматически)"
    else
        echo "  Создан config.yml (ЗАПОЛНИТЕ секреты: mqtt, postgres)"
    fi
    echo "  Редактировать: nano $INSTALL_DIR/config.yml"
else
    echo "  config.yml уже существует"
fi

# Применяем SQL схему автоматически
echo ""
echo "  Применяю SQL схему..."
if "$INSTALL_DIR/venv/bin/python" "$INSTALL_DIR/scripts/setup_db.py" --config "$INSTALL_DIR/config.yml" > /dev/null 2>&1; then
    echo "  Схема применена"
else
    echo "  ⚠ Не удалось применить схему (проверьте config.yml и запустите вручную)"
    echo "    cd $INSTALL_DIR && venv/bin/python scripts/setup_db.py --config config.yml"
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
echo "  1) Проверьте/дополните config.yml (mqtt секция):"
echo "     nano $INSTALL_DIR/config.yml"
echo ""
echo "  2) Проверьте подключения:"
echo "     cd $INSTALL_DIR"
echo "     venv/bin/python scripts/check_health.py --config config.yml"
echo ""
echo "  3) Запустите сервис:"
echo "     sudo systemctl enable --now cg-db-writer"
echo ""
echo "  4) Проверьте логи:"
echo "     sudo journalctl -u cg-db-writer -f"
echo ""
