/**
 * CG DB-Writer — веб-интерфейс конфигурации
 */

let _config = {};
let _configVersion = 0;
let _dirty = false;

// ── Инициализация ──────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", async () => {
  await loadConfig();
  await loadHealth();
  initSectionToggles();
  initButtons();
});

// ── API ────────────────────────────────────────────────────────────────────

async function loadConfig() {
  try {
    const resp = await fetch("/api/config");
    if (!resp.ok) throw new Error(await resp.text());
    const data = await resp.json();
    _configVersion = data.config_version || 0;
    delete data.config_version;
    _config = data;
    renderConfig(_config);
    updateVersionBadge();
    setDirty(false);
  } catch (e) {
    showToast("Ошибка загрузки конфига: " + e.message, "error");
  }
}

async function loadHealth() {
  try {
    const resp = await fetch("/health");
    const data = await resp.json();
    const el = document.getElementById("health-status");
    const cls = data.status === "ok" ? "badge-ok" :
                data.status === "idle" ? "badge-idle" : "badge-dead";
    el.className = "badge " + cls;
    el.textContent = data.status;
    document.getElementById("app-version").textContent = "v" + data.version;
  } catch (e) {
    // Health недоступен — не критично при первом запуске
  }
}

async function saveConfig() {
  const data = collectConfig();
  data.config_version = _configVersion;

  try {
    const resp = await fetch("/api/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });
    const result = await resp.json();
    if (!resp.ok) throw new Error(result.error || "Ошибка сохранения");

    _configVersion = result.config_version;
    updateVersionBadge();
    setDirty(false);
    showBanner("restart-banner", true);
    showToast("Конфиг сохранён (v" + _configVersion + ")", "success");
  } catch (e) {
    showToast("Ошибка: " + e.message, "error");
  }
}

async function restartService() {
  if (!confirm("Перезапустить CG DB-Writer?")) return;
  try {
    await fetch("/api/restart", { method: "POST" });
    showToast("Перезапуск...", "success");
    // Через 3 секунды пробуем перезагрузить страницу
    setTimeout(() => {
      window.location.reload();
    }, 3000);
  } catch (e) {
    // Ожидаемо — соединение оборвётся при перезапуске
    showToast("Сервис перезапускается...", "success");
    setTimeout(() => window.location.reload(), 3000);
  }
}

function downloadConfig() {
  window.location.href = "/api/config/download";
}

function uploadConfig() {
  const input = document.createElement("input");
  input.type = "file";
  input.accept = ".yml,.yaml";
  input.onchange = async () => {
    const file = input.files[0];
    if (!file) return;
    if (!confirm("Загрузить конфиг из файла «" + file.name + "»? Текущий будет заменён.")) return;

    const form = new FormData();
    form.append("file", file);

    try {
      const resp = await fetch("/api/config/upload", { method: "POST", body: form });
      const result = await resp.json();
      if (!resp.ok) throw new Error(result.error || "Ошибка загрузки");

      _configVersion = result.config_version;
      await loadConfig();
      showBanner("restart-banner", true);
      showToast("Конфиг восстановлен (v" + _configVersion + ")", "success");
    } catch (e) {
      showToast("Ошибка: " + e.message, "error");
    }
  };
  input.click();
}

// ── Рендер конфига в форму ─────────────────────────────────────────────────

function renderConfig(cfg) {
  renderSection("mqtt", cfg.mqtt || {}, FIELDS_MQTT);
  renderSection("postgres", cfg.postgres || {}, FIELDS_POSTGRES);
  renderSection("ingest", cfg.ingest || {}, FIELDS_INGEST);
  renderSection("gps_filter", cfg.gps_filter || {}, FIELDS_GPS);
  renderSection("events_policy", cfg.events_policy || {}, FIELDS_EVENTS);
  renderSection("retention", cfg.retention || {}, FIELDS_RETENTION);
  renderSection("logging", cfg.logging || {}, FIELDS_LOGGING);
  renderSection("health", cfg.health || {}, FIELDS_HEALTH);
  renderSection("web_ui", cfg.web_ui || {}, FIELDS_WEBUI);

  // History policy — defaults + KPI
  renderSection("history_defaults", (cfg.history_policy || {}).defaults || {}, FIELDS_HISTORY_DEFAULTS);
  renderKpiTable((cfg.history_policy || {}).kpi_registers || []);
}

/**
 * Описание полей: [key, label, type, hint?]
 * type: "text" | "number" | "password" | "bool" | "select:opt1,opt2"
 */

const FIELDS_MQTT = [
  ["host",               "Хост",              "text"],
  ["port",               "Порт",              "number"],
  ["user",               "Пользователь",      "text"],
  ["password",           "Пароль",            "password"],
  ["tls",                "TLS",               "bool"],
  ["client_id",          "Client ID",         "text"],
  ["keepalive",          "Keepalive (сек)",   "number"],
  ["reconnect_min_delay","Мин. реконнект (сек)","number"],
  ["reconnect_max_delay","Макс. реконнект (сек)","number"],
  ["subscriptions.decoded",  "Подписка decoded",   "text"],
  ["subscriptions.telemetry","Подписка telemetry",  "text"],
];

const FIELDS_POSTGRES = [
  ["host",     "Хост",         "text"],
  ["port",     "Порт",         "number"],
  ["dbname",   "База данных",  "text"],
  ["user",     "Пользователь", "text"],
  ["password", "Пароль",       "password"],
  ["pool_min", "Пул мин.",     "number"],
  ["pool_max", "Пул макс.",    "number"],
];

const FIELDS_INGEST = [
  ["decoded_queue_maxsize",   "Очередь decoded макс.",    "number"],
  ["telemetry_queue_maxsize", "Очередь telemetry макс.",  "number"],
  ["worker_count",            "Кол-во воркеров",          "number"],
  ["drop_decoded_when_full",  "Сбрасывать при заполнении","bool"],
  ["drop_decoded_policy",     "Политика сброса",          "select:drop_oldest,drop_new"],
  ["worker_max_retries",      "Макс. ретраев",            "number"],
  ["worker_retry_delay_sec",  "Задержка ретрая (сек)",    "number"],
];

const FIELDS_GPS = [
  ["sats_min",        "Мин. спутников",         "number"],
  ["fix_min",         "Мин. fix quality",        "number"],
  ["deadband_m",      "Deadband (м)",            "number", "Зона нечувствительности — не обновлять если ближе"],
  ["max_jump_m",      "Макс. прыжок (м)",        "number"],
  ["max_speed_kmh",   "Макс. скорость (км/ч)",   "number"],
  ["confirm_points",  "Точек подтверждения",     "number"],
  ["confirm_radius_m","Радиус подтверждения (м)", "number"],
];

const FIELDS_HISTORY_DEFAULTS = [
  ["tolerance_analog", "Допуск (аналог)",         "number", "Порог изменения для записи"],
  ["min_interval_sec", "Мин. интервал (сек)",      "number", "Защита от flood"],
  ["heartbeat_sec",    "Heartbeat (сек)",          "number", "Запись при неизменном значении"],
  ["store_history",    "Хранить историю",          "bool"],
  ["value_kind",       "Тип значения",             "select:analog,discrete,counter,enum,text"],
];

const FIELDS_EVENTS = [
  ["router_stale_sec",  "Роутер stale (сек)",            "number"],
  ["router_offline_sec","Роутер offline (сек)",           "number"],
  ["panel_stale_sec",   "Панель stale (сек)",             "number"],
  ["panel_offline_sec", "Панель offline (сек)",            "number"],
  ["check_interval_sec","Интервал проверки (сек)",         "number"],
  ["enable_gps_reject_events",        "События GPS reject",       "bool"],
  ["enable_unknown_register_events",  "События неизвестных рег.", "bool"],
];

const FIELDS_RETENTION = [
  ["gps_raw_days",       "GPS raw (дни)",        "number", "Только информационно — политика в TimescaleDB"],
  ["history_raw_days",   "History raw (дни)",     "number"],
  ["history_1min_days",  "History 1min (дни)",    "number"],
  ["history_1hour_years","History 1hour (годы)",  "number"],
];

const FIELDS_LOGGING = [
  ["level",     "Уровень",     "select:DEBUG,INFO,WARNING,ERROR"],
  ["log_file",  "Файл лога",   "text", "Пусто = только stdout"],
  ["json_logs", "JSON формат", "bool"],
];

const FIELDS_HEALTH = [
  ["enabled", "Включен",  "bool"],
  ["bind",    "Bind адрес","text"],
  ["port",    "Порт",      "number"],
];

const FIELDS_WEBUI = [
  ["enabled", "Веб-интерфейс включен", "bool"],
];

function renderSection(sectionId, data, fields) {
  const body = document.getElementById("body-" + sectionId);
  if (!body) return;
  body.innerHTML = "";

  for (const [key, label, type, hint] of fields) {
    // Поддержка nested keys (mqtt.subscriptions.decoded)
    const val = getNestedValue(data, key);
    body.appendChild(createField(sectionId, key, label, type, val, hint));
  }
}

function createField(section, key, label, type, value, hint) {
  const row = document.createElement("div");
  row.className = "field-row";

  const lbl = document.createElement("label");
  lbl.textContent = label;
  row.appendChild(lbl);

  let input;

  if (type === "bool") {
    input = document.createElement("input");
    input.type = "checkbox";
    input.checked = !!value;
    input.dataset.section = section;
    input.dataset.key = key;
    input.addEventListener("change", () => setDirty(true));
  } else if (type.startsWith("select:")) {
    input = document.createElement("select");
    const opts = type.substring(7).split(",");
    for (const opt of opts) {
      const o = document.createElement("option");
      o.value = opt;
      o.textContent = opt;
      if (String(value) === opt) o.selected = true;
      input.appendChild(o);
    }
    input.dataset.section = section;
    input.dataset.key = key;
    input.addEventListener("change", () => setDirty(true));
  } else {
    input = document.createElement("input");
    input.type = type;
    input.value = value != null ? value : "";
    input.dataset.section = section;
    input.dataset.key = key;
    input.addEventListener("input", () => setDirty(true));
  }

  row.appendChild(input);

  if (hint) {
    const h = document.createElement("div");
    h.className = "field-hint";
    h.textContent = hint;
    row.appendChild(h);
  }

  return row;
}

// ── KPI таблица ────────────────────────────────────────────────────────────

function renderKpiTable(kpis) {
  const container = document.getElementById("body-kpi");
  if (!container) return;
  container.innerHTML = "";

  const table = document.createElement("table");
  table.className = "kpi-table";
  table.innerHTML = `
    <thead>
      <tr>
        <th>Адрес</th>
        <th>Тип обор.</th>
        <th>Мин. интервал</th>
        <th>Heartbeat</th>
        <th>Допуск</th>
        <th></th>
      </tr>
    </thead>
    <tbody id="kpi-tbody"></tbody>
  `;
  container.appendChild(table);

  const tbody = table.querySelector("#kpi-tbody");
  for (const kpi of kpis) {
    tbody.appendChild(createKpiRow(kpi));
  }

  const addBtn = document.createElement("button");
  addBtn.className = "btn btn-outline btn-sm";
  addBtn.textContent = "+ Добавить KPI регистр";
  addBtn.style.marginTop = "8px";
  addBtn.addEventListener("click", () => {
    tbody.appendChild(createKpiRow({ addr: 0, equip_type: "pcc", min_interval_sec: 0, heartbeat_sec: 60, tolerance: 0.5 }));
    setDirty(true);
  });
  container.appendChild(addBtn);
}

function createKpiRow(kpi) {
  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td><input type="number" class="kpi-addr" value="${kpi.addr || 0}"></td>
    <td><input type="text" class="kpi-equip" value="${kpi.equip_type || 'pcc'}"></td>
    <td><input type="number" class="kpi-min-interval" value="${kpi.min_interval_sec || 0}"></td>
    <td><input type="number" class="kpi-heartbeat" value="${kpi.heartbeat_sec || 60}"></td>
    <td><input type="number" step="0.01" class="kpi-tolerance" value="${kpi.tolerance || 0}"></td>
    <td><button class="btn btn-outline btn-sm kpi-del">x</button></td>
  `;
  tr.querySelector(".kpi-del").addEventListener("click", () => {
    tr.remove();
    setDirty(true);
  });
  tr.querySelectorAll("input").forEach(i => i.addEventListener("input", () => setDirty(true)));
  return tr;
}

// ── Сбор данных из формы ───────────────────────────────────────────────────

function collectConfig() {
  const cfg = {};
  const inputs = document.querySelectorAll("[data-section][data-key]");

  for (const input of inputs) {
    const section = input.dataset.section;
    const key = input.dataset.key;
    let val;

    if (input.type === "checkbox") {
      val = input.checked;
    } else if (input.type === "number") {
      val = input.value === "" ? 0 : Number(input.value);
    } else {
      val = input.value;
    }

    if (!cfg[section]) cfg[section] = {};
    setNestedValue(cfg[section], key, val);
  }

  // history_policy из defaults + kpi
  cfg.history_policy = {
    defaults: cfg.history_defaults || {},
    kpi_registers: collectKpi(),
  };
  delete cfg.history_defaults;

  return cfg;
}

function collectKpi() {
  const rows = document.querySelectorAll("#kpi-tbody tr");
  const kpis = [];
  for (const row of rows) {
    kpis.push({
      addr: Number(row.querySelector(".kpi-addr").value) || 0,
      equip_type: row.querySelector(".kpi-equip").value || "pcc",
      min_interval_sec: Number(row.querySelector(".kpi-min-interval").value) || 0,
      heartbeat_sec: Number(row.querySelector(".kpi-heartbeat").value) || 60,
      tolerance: Number(row.querySelector(".kpi-tolerance").value) || 0,
    });
  }
  return kpis;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function getNestedValue(obj, key) {
  const parts = key.split(".");
  let current = obj;
  for (const p of parts) {
    if (current == null) return undefined;
    current = current[p];
  }
  return current;
}

function setNestedValue(obj, key, value) {
  const parts = key.split(".");
  let current = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    if (!current[parts[i]]) current[parts[i]] = {};
    current = current[parts[i]];
  }
  current[parts[parts.length - 1]] = value;
}

function setDirty(dirty) {
  _dirty = dirty;
  const btn = document.getElementById("btn-save");
  if (btn) {
    btn.disabled = !dirty;
    btn.textContent = dirty ? "Сохранить *" : "Сохранить";
  }
}

function updateVersionBadge() {
  const el = document.getElementById("config-version");
  if (el) el.textContent = "конфиг v" + _configVersion;
}

function showBanner(id, show) {
  const el = document.getElementById(id);
  if (el) el.classList.toggle("show", show);
}

function showToast(msg, type) {
  let toast = document.getElementById("toast");
  if (!toast) {
    toast = document.createElement("div");
    toast.id = "toast";
    document.body.appendChild(toast);
  }
  toast.className = "toast toast-" + type;
  toast.textContent = msg;
  requestAnimationFrame(() => toast.classList.add("show"));
  setTimeout(() => toast.classList.remove("show"), 3000);
}

// ── Section toggles ────────────────────────────────────────────────────────

function initSectionToggles() {
  document.querySelectorAll(".section-header").forEach(h => {
    h.addEventListener("click", () => {
      h.parentElement.classList.toggle("collapsed");
    });
  });
}

function initButtons() {
  document.getElementById("btn-save")?.addEventListener("click", saveConfig);
  document.getElementById("btn-restart")?.addEventListener("click", restartService);
  document.getElementById("btn-download")?.addEventListener("click", downloadConfig);
  document.getElementById("btn-upload")?.addEventListener("click", uploadConfig);
}
