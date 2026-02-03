from flask import Flask, request, jsonify
import os
import csv
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

import requests
import psycopg2
from psycopg2 import errors as pg_errors

app = Flask(__name__)

# -----------------------------
# ENV VARS - SMSTOOLS
# -----------------------------
SMSTOOLS_CLIENT_ID = os.environ.get("SMSTOOLS_CLIENT_ID")
SMSTOOLS_CLIENT_SECRET = os.environ.get("SMSTOOLS_CLIENT_SECRET")
SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"

# -----------------------------
# ENV VARS - RETELL
# -----------------------------
RETELL_API_KEY = os.environ.get("RETELL_API_KEY")
RETELL_BASE_URL = "https://api.retellai.com"

# -----------------------------
# ENV VARS - DATABASE
# -----------------------------
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()

# -----------------------------
# DEBUG LOGGING
# -----------------------------
DEBUG_LOGS = (os.environ.get("DEBUG_LOGS") or "true").lower() in ("1", "true", "yes", "y")


def log(msg: str) -> None:
    if DEBUG_LOGS:
        print(msg, flush=True)


# -----------------------------
# TENANTS (CSV)
# -----------------------------
# Keyed by virtual_number (receiver) because inbound webhook uses receiver to find tenant
TENANTS_BY_VIRTUAL: Dict[str, Dict[str, Any]] = {}
# Also keyed by tenant_id for easy lookup when returning usage
TENANTS_BY_ID: Dict[str, Dict[str, Any]] = {}

# (tenant_id, phone) -> chat_id
SMS_SESSIONS: Dict[Tuple[str, str], str] = {}


@app.before_request
def log_request():
    try:
        log(f"➡️ {request.method} {request.path}")
    except Exception:
        pass


# -----------------------------
# CSV HELPERS
# -----------------------------
def detect_delimiter(path: str) -> str:
    """
    Detects delimiter between ',' and ';' based on the header line.
    Defaults to ';' if unclear.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            header = f.readline()
        if header.count(",") > header.count(";"):
            return ","
        return ";"
    except Exception:
        return ";"


def load_tenants_from_csv(path: str = "tenants.csv") -> None:
    """
    Supports both old and new header names.
    New: tenant_id,company_name,company_number,virtual_number,retell_agent_id,plan,opening_line
    Old: tenant_id,tenant_name,virtual_number,retell_agent_id,plan,opening_line (delimiter often ;)
    """
    global TENANTS_BY_VIRTUAL, TENANTS_BY_ID
    TENANTS_BY_VIRTUAL = {}
    TENANTS_BY_ID = {}

    if not os.path.exists(path):
        log("⚠️ tenants.csv niet gevonden")
        return

    delimiter = detect_delimiter(path)

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            # virtual_number is the key used by smstools inbound receiver
            virtual = (row.get("virtual_number") or "").strip()
            if not virtual:
                continue

            tenant_id = (row.get("tenant_id") or "").strip() or virtual

            # allow both tenant_name and company_name
            company_name = (row.get("company_name") or row.get("tenant_name") or "").strip() or tenant_id

            # optional
            company_number = (row.get("company_number") or "").strip()
            plan = (row.get("plan") or "").strip()
            retell_agent_id = (row.get("retell_agent_id") or "").strip()
            opening_line = (row.get("opening_line") or "").strip()

            tenant = {
                "tenant_id": tenant_id,
                "company_name": company_name,
                "company_number": company_number,
                "plan": plan,
                "virtual_number": virtual,
                "retell_agent_id": retell_agent_id,
                "opening_line": opening_line,
            }

            TENANTS_BY_VIRTUAL[virtual] = tenant
            TENANTS_BY_ID[tenant_id] = tenant

    log(f"✅ {len(TENANTS_BY_VIRTUAL)} tenants geladen (delimiter='{delimiter}')")


def get_tenant_by_receiver(receiver: str) -> Optional[Dict[str, Any]]:
    if not receiver:
        return None
    return TENANTS_BY_VIRTUAL.get(receiver.strip())


# -----------------------------
# DB HELPERS
# -----------------------------
def month_key() -> str:
    return datetime.utcnow().strftime("%Y-%m")


def db_available() -> bool:
    return bool(DATABASE_URL)


def ensure_monthly_usage_table() -> None:
    if not db_available():
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
        log("✅ DB table monthly_usage is aanwezig")
    except Exception as e:
        log(f"⚠️ Kon monthly_usage table niet verzekeren: {e}")


def bump_monthly_outbound(tenant_id: str) -> None:
    if not db_available():
        return

    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO monthly_usage (month, tenant_id, outbound_count)
                    VALUES (%s, %s, 1)
                    ON CONFLICT (month, tenant_id)
                    DO UPDATE SET outbound_count = monthly_usage.outbound_count + 1,
                                  updated_at = NOW();
                    """,
                    (month_key(), tenant_id),
                )
    except pg_errors.UndefinedTable:
        ensure_monthly_usage_table()
        try:
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO monthly_usage (month, tenant_id, outbound_count)
                        VALUES (%s, %s, 1)
                        ON CONFLICT (month, tenant_id)
                        DO UPDATE SET outbound_count = monthly_usage.outbound_count + 1,
                                      updated_at = NOW();
                        """,
                        (month_key(), tenant_id),
                    )
        except Exception as e:
            log(f"⚠️ bump_monthly_outbound retry faalde: {e}")
    except Exception as e:
        log(f"⚠️ bump_monthly_outbound error: {e}")


# -----------------------------
# SMS VERSTUREN
# -----------------------------
def send_sms(tenant: Dict[str, Any], to_number: str, message: str) -> None:
    """
    Stuurt via smstools en telt dan outbound (1 per sms).
    """
    if not to_number or not message:
        return

    if not SMSTOOLS_CLIENT_ID or not SMSTOOLS_CLIENT_SECRET:
        log("⚠️ SMSTOOLS_CLIENT_ID/SECRET ontbreken")
        return

    payload = {
        "message": message,
        "to": to_number,
        "sender": tenant["virtual_number"],  # juiste virtual number
    }
    headers = {
        "X-Client-Id": SMSTOOLS_CLIENT_ID,
        "X-Client-Secret": SMSTOOLS_CLIENT_SECRET,
        "Content-Type": "application/json",
    }

    try:
        r = requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers, timeout=15)
        if 200 <= r.status_code < 300:
            bump_monthly_outbound(tenant["tenant_id"])
        else:
            log(f"⚠️ SMS send faalde status={r.status_code} body={r.text[:200]}")
    except Exception as e:
        log(f"⚠️ SMS send error: {e}")


# -----------------------------
# RETELL
# -----------------------------
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
            json={
                "agent_id": tenant["retell_agent_id"],
                "metadata": {"phone": phone},
            },
            timeout=15,
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
            timeout=20,
        )
        data = r.json() if r.content else {}

        for m in reversed(data.get("messages", [])):
            if m.get("role") == "agent":
                content = (m.get("content") or "").strip()
                return content or opening

    except Exception as e:
        log(f"⚠️ Retell completion error: {e}")

    return opening


# -----------------------------
# ROUTES
# -----------------------------
@app.route("/", methods=["GET"])
def health():
    return "OK", 200


@app.route("/sms/inbound", methods=["POST"])
def sms_inbound():
    """
    Smstools webhook.
    We verwachten dict of list.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return "OK", 200

    event = data[0] if isinstance(data, list) and data else data
    if not isinstance(event, dict):
        return "OK", 200

    webhook_type = event.get("webhook_type")
    msg = event.get("message") or {}
    receiver = (msg.get("receiver") or "").strip()

    tenant = get_tenant_by_receiver(receiver)
    if not tenant:
        return "OK", 200

    if webhook_type == "inbox_message":
        sender = (msg.get("sender") or "").strip()
        text = (msg.get("content") or "").strip()

        if sender and text:
            reply = ask_retell_via_sms(tenant, sender, text)
            send_sms(tenant, sender, reply)

    return "OK", 200


# -----------------------------
# ADMIN API — USAGE (voor Google Sheets)
# -----------------------------
@app.route("/admin/usage", methods=["GET"])
def admin_usage():
    """
    Geeft maandelijkse outbound counts + tenant info terug.
    """
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
        for (m, tenant_id, c) in rows:
            t = TENANTS_BY_ID.get(tenant_id, {})
            data.append(
                {
                    "month": m,
                    "tenant_id": tenant_id,
                    "company_name": t.get("company_name", ""),
                    "company_number": t.get("company_number", ""),
                    "plan": t.get("plan", ""),
                    "outbound_count": c,
                }
            )

        return jsonify({"data": data}), 200

    except pg_errors.InvalidCatalogName as e:
        log(f"❌ DB bestaat niet (InvalidCatalogName): {e}")
        return jsonify({"error": "database does not exist", "data": []}), 500

    except Exception as e:
        log(f"❌ admin_usage error: {e}")
        return jsonify({"error": "internal_error", "data": []}), 500


# -----------------------------
# STARTUP
# -----------------------------
load_tenants_from_csv()
ensure_monthly_usage_table()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))

