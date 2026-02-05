import os
import re
import csv
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import requests
import psycopg2
from psycopg2 import errors as pg_errors
from flask import Flask, request, jsonify

app = Flask(__name__)

# ============================================================
# ENV / CONFIG
# ============================================================

# Smstools
SMSTOOLS_CLIENT_ID = (os.environ.get("SMSTOOLS_CLIENT_ID") or "").strip()
SMSTOOLS_CLIENT_SECRET = (os.environ.get("SMSTOOLS_CLIENT_SECRET") or "").strip()
SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"

# Retell
RETELL_API_KEY = (os.environ.get("RETELL_API_KEY") or "").strip()
RETELL_BASE_URL = "https://api.retellai.com"

# DB
RAW_DATABASE_URL = os.environ.get("DATABASE_URL") or ""

# Admin
ADMIN_TOKEN = (os.environ.get("ADMIN_TOKEN") or "").strip()

# CSV
TENANTS_CSV_PATH = os.environ.get("TENANTS_CSV_PATH") or "tenants.csv"

# Logs
DEBUG_LOGS = (os.environ.get("DEBUG_LOGS") or "true").lower() in ("1", "true", "yes", "y")


def log(msg: str) -> None:
    if DEBUG_LOGS:
        print(msg, flush=True)


@app.before_request
def _log_request() -> None:
    try:
        log(f"➡️ {request.method} {request.path} qs={request.query_string.decode('utf-8', 'ignore')}")
    except Exception:
        pass


def sanitize_database_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    m = re.search(r"(postgres(?:ql)?://\S+)", raw)
    return (m.group(1).strip() if m else raw.strip())


DATABASE_URL = sanitize_database_url(RAW_DATABASE_URL)


def db_available() -> bool:
    return bool(DATABASE_URL)


def _mask_token(t: str) -> str:
    t = (t or "").strip()
    if len(t) <= 8:
        return "*" * len(t)
    return t[:4] + "..." + t[-4:]


def _extract_admin_token_from_request() -> str:
    """
    Accept token in:
      - query param: ?token=...
      - header: Authorization: Bearer <token>
      - header: X-Admin-Token: <token>
      - header: X-API-Key: <token>
    """
    # 1) query
    token = (request.args.get("token") or "").strip()
    if token:
        return token

    # 2) Authorization Bearer
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()

    # 3) custom headers
    token = (request.headers.get("X-Admin-Token") or "").strip()
    if token:
        return token

    token = (request.headers.get("X-API-Key") or "").strip()
    if token:
        return token

    return ""


def require_admin_token() -> Optional[Any]:
    """
    If ADMIN_TOKEN is empty -> no auth required (NOT recommended for prod).
    """
    if not ADMIN_TOKEN:
        log("⚠️ ADMIN_TOKEN is empty -> admin endpoints are OPEN (set ADMIN_TOKEN in Render env).")
        return None

    provided = _extract_admin_token_from_request()

    # Debug info (masked)
    log(f"🔐 Admin auth check: expected={_mask_token(ADMIN_TOKEN)} provided={_mask_token(provided)} "
        f"via={'query' if request.args.get('token') else 'headers' if provided else 'none'}")

    if not provided or provided != ADMIN_TOKEN:
        return jsonify({"error": "unauthorized"}), 401

    return None


def month_key(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.utcnow()
    return d.strftime("%Y-%m")


def normalize_phone(raw: str) -> str:
    s = re.sub(r"\D+", "", (raw or "").strip())
    if s.startswith("0032"):
        s = "32" + s[4:]
    if s.startswith("0") and len(s) in (9, 10):
        s = "32" + s[1:]
    return s


def detect_csv_delimiter(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            header = f.readline()
        return ";" if header.count(";") >= header.count(",") else ","
    except Exception:
        return ";"


def to_int_safe(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        s = str(value).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


# ============================================================
# TENANTS (CSV)
# ============================================================

TENANTS_BY_VIRTUAL: Dict[str, Dict[str, Any]] = {}
TENANTS_BY_ID: Dict[str, Dict[str, Any]] = {}

# (tenant_id, phone) -> chat_id
SMS_SESSIONS: Dict[Tuple[str, str], str] = {}


def load_tenants_from_csv(path: str) -> None:
    global TENANTS_BY_VIRTUAL, TENANTS_BY_ID
    TENANTS_BY_VIRTUAL = {}
    TENANTS_BY_ID = {}

    if not os.path.exists(path):
        log(f"⚠️ tenants.csv not found at {path}")
        return

    delimiter = detect_csv_delimiter(path)
    log(f"ℹ️ tenants.csv delimiter='{delimiter}' path={path}")

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            tenant_id = (row.get("tenant_id") or "").strip()
            virtual_raw = (row.get("virtual_number") or "").strip()
            if not tenant_id or not virtual_raw:
                continue

            tenant = {
                "tenant_id": tenant_id,
                "stripe_customer_id": (row.get("stripe_customer_id") or "").strip(),
                "company_name": (row.get("company_name") or "").strip(),
                "company_number": (row.get("company_number") or "").strip(),
                "virtual_number": virtual_raw,
                "retell_agent_id": (row.get("retell_agent_id") or "").strip(),
                "plan": (row.get("plan") or "").strip().lower(),
                "price_cents": to_int_safe(row.get("price_cents"), 0),
                "opening_line": (row.get("opening_line") or "").strip(),
            }

            TENANTS_BY_VIRTUAL[normalize_phone(virtual_raw)] = tenant
            TENANTS_BY_ID[tenant_id] = tenant

    log(f"✅ Loaded tenants: {len(TENANTS_BY_ID)}")


def get_tenant_by_receiver(receiver: str) -> Optional[Dict[str, Any]]:
    return TENANTS_BY_VIRTUAL.get(normalize_phone(receiver or ""))


def get_overage_price_cents(tenant: Dict[str, Any]) -> int:
    pc = to_int_safe(tenant.get("price_cents"), 0)
    if pc > 0:
        return pc
    plan = (tenant.get("plan") or "").strip().lower()
    return 17 if plan == "advanced" else 19


# ============================================================
# DB: monthly_usage
# ============================================================

def ensure_monthly_usage_table() -> None:
    if not db_available():
        log("⚠️ DATABASE_URL missing; usage tracking disabled")
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS monthly_usage (
                      month TEXT NOT NULL,
                      tenant_id TEXT NOT NULL,
                      outbound_count INT NOT NULL DEFAULT 0,
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      PRIMARY KEY (month, tenant_id)
                    );
                    """
                )
        log("✅ monthly_usage ensured")
    except Exception as e:
        log(f"⚠️ ensure_monthly_usage_table failed: {e}")


def bump_monthly_outbound(tenant_id: str, amount: int = 1) -> None:
    if not db_available():
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO monthly_usage (month, tenant_id, outbound_count)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (month, tenant_id)
                    DO UPDATE SET outbound_count = monthly_usage.outbound_count + EXCLUDED.outbound_count,
                                  updated_at = NOW();
                    """,
                    (month_key(), tenant_id, int(amount)),
                )
        log(f"✅ outbound+{amount} tenant={tenant_id} month={month_key()}")
    except pg_errors.UndefinedTable:
        ensure_monthly_usage_table()
        bump_monthly_outbound(tenant_id, amount)
    except Exception as e:
        log(f"⚠️ bump_monthly_outbound failed: {e}")


# ============================================================
# SMS SEND (Smstools)
# ============================================================

def send_sms(tenant: Dict[str, Any], to_number: str, message: str) -> None:
    if not to_number or not message:
        return
    if not SMSTOOLS_CLIENT_ID or not SMSTOOLS_CLIENT_SECRET:
        log("⚠️ Smstools credentials missing")
        return

    payload = {"message": message, "to": to_number, "sender": tenant["virtual_number"]}
    headers = {
        "X-Client-Id": SMSTOOLS_CLIENT_ID,
        "X-Client-Secret": SMSTOOLS_CLIENT_SECRET,
        "Content-Type": "application/json",
    }

    try:
        r = requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers, timeout=25)
        log(f"📤 Smstools send status={r.status_code}")
        if 200 <= r.status_code < 300:
            bump_monthly_outbound(tenant["tenant_id"], 1)
        else:
            log(f"⚠️ Smstools send failed: {r.text[:300]}")
    except Exception as e:
        log(f"⚠️ Smstools send error: {e}")


# ============================================================
# RETELL
# ============================================================

def get_or_create_chat_id(tenant: Dict[str, Any], phone: str) -> Optional[str]:
    key = (tenant["tenant_id"], phone)
    if key in SMS_SESSIONS:
        return SMS_SESSIONS[key]

    if not RETELL_API_KEY or not tenant.get("retell_agent_id"):
        return None

    try:
        r = requests.post(
            f"{RETELL_BASE_URL}/create-chat",
            headers={"Authorization": f"Bearer {RETELL_API_KEY}", "Content-Type": "application/json"},
            json={"agent_id": tenant["retell_agent_id"], "metadata": {"phone": phone}},
            timeout=25,
        )
        data = r.json() if r.content else {}
        chat_id = data.get("chat_id") or data.get("id")
        if chat_id:
            SMS_SESSIONS[key] = chat_id
            return chat_id
    except Exception as e:
        log(f"⚠️ Retell create-chat error: {e}")

    return None


def ask_retell_via_sms(tenant: Dict[str, Any], phone: str, text: str) -> str:
    opening = tenant.get("opening_line") or "Bedankt voor je bericht."

    if not RETELL_API_KEY or not tenant.get("retell_agent_id"):
        return opening

    chat_id = get_or_create_chat_id(tenant, phone)
    if not chat_id:
        return opening

    try:
        r = requests.post(
            f"{RETELL_BASE_URL}/create-chat-completion",
            headers={"Authorization": f"Bearer {RETELL_API_KEY}", "Content-Type": "application/json"},
            json={"chat_id": chat_id, "content": text},
            timeout=30,
        )
        data = r.json() if r.content else {}

        for m in reversed(data.get("messages", [])):
            if m.get("role") == "agent":
                content = (m.get("content") or "").strip()
                return content or opening
    except Exception as e:
        log(f"⚠️ Retell completion error: {e}")

    return opening


# ============================================================
# WEBHOOK PARSING
# ============================================================

def extract_event(payload: Any) -> Optional[Dict[str, Any]]:
    if payload is None:
        return None
    if isinstance(payload, list):
        return payload[0] if payload else None
    if isinstance(payload, dict):
        return payload
    return None


def extract_receiver(event: Dict[str, Any]) -> str:
    msg = event.get("message") or {}
    return (msg.get("receiver") or event.get("receiver") or "").strip()


def extract_sender(event: Dict[str, Any]) -> str:
    msg = event.get("message") or {}
    return (msg.get("sender") or event.get("sender") or event.get("from") or "").strip()


def extract_text(event: Dict[str, Any]) -> str:
    msg = event.get("message") or {}
    return (msg.get("content") or event.get("content") or event.get("text") or "").strip()


def extract_calling_number(event: Dict[str, Any]) -> str:
    return ((event.get("caller") or "") or (event.get("from") or "") or extract_sender(event)).strip()


# ============================================================
# ROUTES
# ============================================================

@app.route("/", methods=["GET"])
def health():
    return "OK", 200


@app.route("/admin/ping", methods=["GET"])
def admin_ping():
    auth = require_admin_token()
    if auth is not None:
        return auth
    return jsonify({"ok": True}), 200


@app.route("/sms/inbound", methods=["POST"])
def sms_inbound():
    payload = request.get_json(force=True, silent=True)
    event = extract_event(payload)
    if not event:
        return "OK", 200

    receiver = extract_receiver(event)
    sender = extract_sender(event)
    text = extract_text(event)

    tenant = get_tenant_by_receiver(receiver)
    if not tenant:
        log(f"⚠️ /sms/inbound: no tenant for receiver={receiver} norm={normalize_phone(receiver)}")
        return "OK", 200

    if sender and text:
        reply = ask_retell_via_sms(tenant, sender, text)
        send_sms(tenant, sender, reply)

    return "OK", 200


@app.route("/call/missed", methods=["POST"])
def call_missed():
    payload = request.get_json(force=True, silent=True)
    event = extract_event(payload)
    if not event:
        return "OK", 200

    receiver = extract_receiver(event)
    caller = extract_calling_number(event)

    tenant = get_tenant_by_receiver(receiver)
    if not tenant:
        log(f"⚠️ /call/missed: no tenant for receiver={receiver} norm={normalize_phone(receiver)}")
        return "OK", 200

    if caller:
        opening = tenant.get("opening_line") or "Bedankt om te bellen. Hoe kan ik helpen?"
        send_sms(tenant, caller, opening)

    return "OK", 200


# ============================================================
# ADMIN: Usage (for Google Sheets)
# ============================================================

@app.route("/admin/usage", methods=["GET"])
def admin_usage():
    auth = require_admin_token()
    if auth is not None:
        return auth

    if not db_available():
        return jsonify({"data": []}), 200

    try:
        ensure_monthly_usage_table()

        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT month, tenant_id, outbound_count
                    FROM monthly_usage
                    ORDER BY month DESC, tenant_id;
                    """
                )
                rows = cur.fetchall()

        data = []
        for (m, tenant_id, outbound) in rows:
            t = TENANTS_BY_ID.get(tenant_id, {})
            pc = get_overage_price_cents(t)

            data.append(
                {
                    "month": m,
                    "company_number": t.get("company_number", ""),
                    "company_name": t.get("company_name", ""),
                    "tenant_id": tenant_id,
                    "stripe_customer_id": t.get("stripe_customer_id", ""),
                    "plan": t.get("plan", ""),
                    "outbound": int(outbound or 0),
                    "price_cents": pc,
                    "price_eur": pc / 100.0,
                }
            )

        return jsonify({"data": data}), 200

    except Exception as e:
        log(f"❌ /admin/usage error: {e}")
        return jsonify({"error": "internal_error", "data": []}), 500


@app.route("/admin/reload-tenants", methods=["POST"])
def admin_reload_tenants():
    auth = require_admin_token()
    if auth is not None:
        return auth

    load_tenants_from_csv(TENANTS_CSV_PATH)
    return jsonify({"ok": True, "tenants": len(TENANTS_BY_ID)}), 200


@app.route("/admin/bump", methods=["POST"])
def admin_bump():
    auth = require_admin_token()
    if auth is not None:
        return auth

    payload = request.get_json(force=True, silent=True) or {}
    tenant_id = (payload.get("tenant_id") or "").strip()
    amount = to_int_safe(payload.get("amount"), 1)

    if not tenant_id:
        return jsonify({"error": "tenant_id required"}), 400

    bump_monthly_outbound(tenant_id, amount)
    return jsonify({"ok": True}), 200


# ============================================================
# STARTUP
# ============================================================

load_tenants_from_csv(TENANTS_CSV_PATH)
ensure_monthly_usage_table()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
