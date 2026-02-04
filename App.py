import os
import re
import csv
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import requests
import psycopg2
from psycopg2 import errors as pg_errors
from flask import Flask, request, jsonify

import stripe  # pip install stripe

app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================

# ---- SMSTOOLS ----
SMSTOOLS_CLIENT_ID = (os.environ.get("SMSTOOLS_CLIENT_ID") or "").strip()
SMSTOOLS_CLIENT_SECRET = (os.environ.get("SMSTOOLS_CLIENT_SECRET") or "").strip()
SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"

# ---- RETELL (optional) ----
RETELL_API_KEY = (os.environ.get("RETELL_API_KEY") or "").strip()
RETELL_BASE_URL = "https://api.retellai.com"

# ---- STRIPE ----
STRIPE_SECRET_KEY = (os.environ.get("STRIPE_SECRET_KEY") or "").strip()

# ---- DB ----
RAW_DATABASE_URL = os.environ.get("DATABASE_URL") or ""

# ---- ADMIN ----
ADMIN_TOKEN = (os.environ.get("ADMIN_TOKEN") or "").strip()

# ---- CSV PATH ----
TENANTS_CSV_PATH = os.environ.get("TENANTS_CSV_PATH") or "tenants.csv"

# ---- LOGGING ----
DEBUG_LOGS = (os.environ.get("DEBUG_LOGS") or "true").lower() in ("1", "true", "yes", "y")


def log(msg: str) -> None:
    if DEBUG_LOGS:
        print(msg, flush=True)


@app.before_request
def _log_request() -> None:
    try:
        log(f"➡️ {request.method} {request.path}")
    except Exception:
        pass


# ============================================================
# HELPERS
# ============================================================

def sanitize_database_url(raw: str) -> str:
    """
    - trims whitespace/newlines
    - if multiple URLs accidentally pasted, pick the first postgresql://... token
    """
    raw = (raw or "").strip()
    if not raw:
        return ""
    m = re.search(r"(postgres(?:ql)?://\S+)", raw)
    return (m.group(1).strip() if m else raw.strip())


DATABASE_URL = sanitize_database_url(RAW_DATABASE_URL)


def db_available() -> bool:
    return bool(DATABASE_URL)


def month_key(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.utcnow()
    return d.strftime("%Y-%m")


def previous_month_key() -> str:
    now = datetime.now(timezone.utc)
    y = now.year
    m = now.month - 1
    if m == 0:
        m = 12
        y -= 1
    return f"{y:04d}-{m:02d}"


def normalize_phone(raw: str) -> str:
    """
    Normalize phone numbers to digits only and try to make BE format consistent:
    - keep digits only
    - convert 0032xxxxxxxxx -> 32xxxxxxxxx
    - convert 0xxxxxxxxx (BE) -> 32xxxxxxxxx (when length suggests BE mobile/landline)
    """
    s = re.sub(r"\D+", "", (raw or "").strip())

    if s.startswith("0032"):
        s = "32" + s[4:]

    # If number looks like Belgian national format starting with 0
    if s.startswith("0") and len(s) in (9, 10):
        s = "32" + s[1:]

    return s


def detect_csv_delimiter(path: str) -> str:
    """
    Simple delimiter detection: if header contains more ';' than ',', treat as ';'
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            header = f.readline()
        return ";" if header.count(";") >= header.count(",") else ","
    except Exception:
        return ";"


def require_admin_token() -> Optional[Any]:
    """
    If ADMIN_TOKEN is set, require ?token=... for admin endpoints.
    """
    if not ADMIN_TOKEN:
        return None
    token = (request.args.get("token") or "").strip()
    if token != ADMIN_TOKEN:
        return jsonify({"error": "unauthorized"}), 401
    return None


# ============================================================
# TENANTS (CSV)
# ============================================================

TENANTS_BY_VIRTUAL: Dict[str, Dict[str, Any]] = {}  # normalized virtual_number -> tenant dict
TENANTS_BY_ID: Dict[str, Dict[str, Any]] = {}       # tenant_id -> tenant dict

# Simple in-memory chat session mapping for Retell:
SMS_SESSIONS: Dict[Tuple[str, str], str] = {}       # (tenant_id, phone) -> chat_id


def load_tenants_from_csv(path: str) -> None:
    """
    Expected columns:
    tenant_id,stripe_customer_id,company_name,company_number,virtual_number,retell_agent_id,plan,opening_line
    delimiter can be ',' or ';'
    """
    global TENANTS_BY_VIRTUAL, TENANTS_BY_ID
    TENANTS_BY_VIRTUAL = {}
    TENANTS_BY_ID = {}

    if not os.path.exists(path):
        log(f"⚠️ tenants.csv not found at: {path}")
        return

    delimiter = detect_csv_delimiter(path)
    log(f"ℹ️ tenants.csv delimiter detected: '{delimiter}' ({path})")

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            virtual_raw = (row.get("virtual_number") or "").strip()
            if not virtual_raw:
                continue

            tenant_id = (row.get("tenant_id") or "").strip() or virtual_raw
            stripe_customer_id = (row.get("stripe_customer_id") or "").strip()

            tenant = {
                "tenant_id": tenant_id,
                "stripe_customer_id": stripe_customer_id,
                "company_name": (row.get("company_name") or "").strip(),
                "company_number": (row.get("company_number") or "").strip(),
                "virtual_number": virtual_raw,
                "retell_agent_id": (row.get("retell_agent_id") or "").strip(),
                "plan": (row.get("plan") or "").strip(),
                "opening_line": (row.get("opening_line") or "").strip(),
            }

            TENANTS_BY_VIRTUAL[normalize_phone(virtual_raw)] = tenant
            TENANTS_BY_ID[tenant_id] = tenant

    log(f"✅ Loaded tenants: {len(TENANTS_BY_VIRTUAL)}")


def get_tenant_by_receiver(receiver: str) -> Optional[Dict[str, Any]]:
    if not receiver:
        return None
    return TENANTS_BY_VIRTUAL.get(normalize_phone(receiver))


# ============================================================
# DB (monthly_usage)
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
        log("✅ monthly_usage table ensured")
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
        except Exception as e:
            log(f"⚠️ bump_monthly_outbound retry failed: {e}")
    except Exception as e:
        log(f"⚠️ bump_monthly_outbound failed: {e}")


def fetch_month_usage(month: str) -> List[Tuple[str, int]]:
    """
    Returns list of (tenant_id, outbound_count) for a given month.
    """
    if not db_available():
        return []
    ensure_monthly_usage_table()

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tenant_id, outbound_count
                FROM monthly_usage
                WHERE month = %s
                ORDER BY tenant_id;
                """,
                (month,),
            )
            return [(r[0], int(r[1] or 0)) for r in cur.fetchall()]


# ============================================================
# STRIPE OVERAGES (invoice items)
# ============================================================

PLAN_LIMITS = {"basic": 200, "advanced": 400}


def ensure_overage_runs_table() -> None:
    """
    Prevents double-billing: one run per (month, tenant_id).
    """
    if not db_available():
        return
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stripe_overage_runs (
                    month TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (month, tenant_id)
                );
                """
            )


def already_ran_overage(month: str, tenant_id: str) -> bool:
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM stripe_overage_runs WHERE month=%s AND tenant_id=%s",
                (month, tenant_id),
            )
            return cur.fetchone() is not None


def mark_ran_overage(month: str, tenant_id: str) -> None:
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stripe_overage_runs (month, tenant_id)
                VALUES (%s, %s)
                ON CONFLICT (month, tenant_id) DO NOTHING;
                """,
                (month, tenant_id),
            )


def stripe_ready() -> bool:
    return bool(STRIPE_SECRET_KEY)


# ============================================================
# SMS SEND (Smstools)
# ============================================================

def send_sms(tenant: Dict[str, Any], to_number: str, message: str) -> None:
    """
    Sends SMS via Smstools and increments outbound_count by 1 on success.
    """
    if not to_number or not message:
        log("⚠️ send_sms: missing to_number/message")
        return

    if not SMSTOOLS_CLIENT_ID or not SMSTOOLS_CLIENT_SECRET:
        log("⚠️ Smstools credentials missing")
        return

    payload = {
        "message": message,
        "to": to_number,
        "sender": tenant["virtual_number"],
    }
    headers = {
        "X-Client-Id": SMSTOOLS_CLIENT_ID,
        "X-Client-Secret": SMSTOOLS_CLIENT_SECRET,
        "Content-Type": "application/json",
    }

    try:
        r = requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers, timeout=20)
        log(f"📤 Smstools send status={r.status_code}")
        if 200 <= r.status_code < 300:
            bump_monthly_outbound(tenant["tenant_id"], 1)
        else:
            log(f"⚠️ Smstools send failed: {r.text[:300]}")
    except Exception as e:
        log(f"⚠️ Smstools send error: {e}")


# ============================================================
# RETELL (optional)
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
            headers={
                "Authorization": f"Bearer {RETELL_API_KEY}",
                "Content-Type": "application/json",
            },
            json={"agent_id": tenant["retell_agent_id"], "metadata": {"phone": phone}},
            timeout=20,
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
            headers={
                "Authorization": f"Bearer {RETELL_API_KEY}",
                "Content-Type": "application/json",
            },
            json={"chat_id": chat_id, "content": text},
            timeout=25,
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
def health() -> Tuple[str, int]:
    return "OK", 200


@app.route("/sms/inbound", methods=["POST"])
def sms_inbound() -> Tuple[str, int]:
    payload = request.get_json(force=True, silent=True)
    event = extract_event(payload)
    if not event:
        return "OK", 200

    receiver = extract_receiver(event)
    sender = extract_sender(event)
    text = extract_text(event)

    log(f"📩 /sms/inbound receiver={receiver} sender={sender} text_len={len(text)}")

    tenant = get_tenant_by_receiver(receiver)
    if not tenant:
        log(f"⚠️ /sms/inbound: no tenant for receiver={receiver} norm={normalize_phone(receiver)}")
        return "OK", 200

    if sender and text:
        reply = ask_retell_via_sms(tenant, sender, text)
        send_sms(tenant, sender, reply)

    return "OK", 200


@app.route("/call/missed", methods=["POST"])
def call_missed() -> Tuple[str, int]:
    payload = request.get_json(force=True, silent=True)
    event = extract_event(payload)
    if not event:
        return "OK", 200

    receiver = extract_receiver(event)
    caller = extract_calling_number(event)

    log(f"📞 /call/missed receiver={receiver} caller={caller}")

    tenant = get_tenant_by_receiver(receiver)
    if not tenant:
        log(f"⚠️ /call/missed: no tenant for receiver={receiver} norm={normalize_phone(receiver)}")
        return "OK", 200

    if caller:
        opening = tenant.get("opening_line") or "Bedankt om te bellen. Hoe kan ik helpen?"
        send_sms(tenant, caller, opening)

    return "OK", 200


# ============================================================
# ADMIN API (Google Sheets usage)
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

        out = []
        for (m, tenant_id, outbound) in rows:
            t = TENANTS_BY_ID.get(tenant_id, {})
            out.append(
                {
                    "month": m,
                    "company_number": t.get("company_number", ""),
                    "company_name": t.get("company_name", ""),
                    "tenant_id": tenant_id,
                    "stripe_customer_id": t.get("stripe_customer_id", ""),
                    "plan": t.get("plan", ""),
                    "outbound": int(outbound or 0),
                }
            )
        return jsonify({"data": out}), 200

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
    amount = int(payload.get("amount") or 1)

    if not tenant_id:
        return jsonify({"error": "tenant_id required"}), 400

    bump_monthly_outbound(tenant_id, amount)
    return jsonify({"ok": True}), 200


# ============================================================
# ADMIN STRIPE (invoice items overages)
# ============================================================

@app.route("/admin/stripe/add-overages", methods=["POST"])
def admin_stripe_add_overages():
    """
    Adds invoice items to Stripe for overages (extra messages) for a given month.

    Body (optional):
      {
        "month": "2026-02",       // default: previous month
        "price_cents": 17         // default: 17
      }
    """
    auth = require_admin_token()
    if auth is not None:
        return auth

    if not stripe_ready():
        return jsonify({"error": "STRIPE_SECRET_KEY missing"}), 400
    if not db_available():
        return jsonify({"error": "DATABASE_URL missing"}), 400

    stripe.api_key = STRIPE_SECRET_KEY
    ensure_monthly_usage_table()
    ensure_overage_runs_table()

    payload = request.get_json(force=True, silent=True) or {}
    month = (payload.get("month") or "").strip() or previous_month_key()
    price_cents = int(payload.get("price_cents") or 17)

    rows = fetch_month_usage(month)

    result = {
        "month": month,
        "price_cents": price_cents,
        "created": [],
        "skipped": {"no_customer": 0, "already_run": 0, "no_extra": 0},
    }

    for tenant_id, outbound in rows:
        tenant = TENANTS_BY_ID.get(tenant_id) or {}
        stripe_customer_id = (tenant.get("stripe_customer_id") or "").strip()
        plan = (tenant.get("plan") or "").strip().lower()

        if not stripe_customer_id:
            result["skipped"]["no_customer"] += 1
            continue

        if already_ran_overage(month, tenant_id):
            result["skipped"]["already_run"] += 1
            continue

        limit = PLAN_LIMITS.get(plan, 0)
        extra = max(0, int(outbound) - int(limit))

        if extra <= 0:
            result["skipped"]["no_extra"] += 1
            mark_ran_overage(month, tenant_id)
            continue

        amount_cents = extra * price_cents
        desc = f"Extra berichten ({month}): {extra} × €{price_cents/100:.2f}"

        try:
            item = stripe.InvoiceItem.create(
                customer=stripe_customer_id,
                amount=amount_cents,
                currency="eur",
                description=desc,
                metadata={
                    "tenant_id": tenant_id,
                    "month": month,
                    "extra_count": str(extra),
                    "price_cents": str(price_cents),
                },
            )
            mark_ran_overage(month, tenant_id)

            result["created"].append(
                {
                    "tenant_id": tenant_id,
                    "stripe_customer_id": stripe_customer_id,
                    "extra": extra,
                    "amount_cents": amount_cents,
                    "invoice_item_id": item.get("id"),
                }
            )
        except Exception as e:
            # Do NOT mark as ran if Stripe failed (so you can retry)
            log(f"❌ Stripe invoice item create failed tenant={tenant_id}: {e}")

    return jsonify(result), 200


# ============================================================
# STARTUP
# ============================================================

load_tenants_from_csv(TENANTS_CSV_PATH)
ensure_monthly_usage_table()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
