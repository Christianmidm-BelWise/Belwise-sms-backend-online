from flask import Flask, request
import requests
import os
import sys
import csv
from typing import Dict, Any, Optional
from datetime import datetime
import psycopg2

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
DATABASE_URL = os.environ.get("DATABASE_URL")

# -----------------------------
# TENANTS (CSV)
# -----------------------------
TENANTS: Dict[str, Dict[str, Any]] = {}
# key: (tenant_id, phone) -> chat_id
SMS_SESSIONS: Dict[tuple, str] = {}


# -----------------------------
# ALWAYS LOG REQUESTS (Render logs)
# -----------------------------
@app.before_request
def _log_every_request():
    # Dit zorgt dat je ALTIJD "POST /sms/inbound" ziet in Render logs
    try:
        print(f"➡️ {request.method} {request.path}", flush=True)
    except Exception:
        pass


# -----------------------------
# CSV LADEN
# -----------------------------
def load_tenants_from_csv(path: str = "tenants.csv") -> None:
    global TENANTS
    TENANTS = {}

    if not os.path.exists(path):
        print(f"⚠️ tenants.csv niet gevonden: {path}", flush=True)
        return

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            virtual = (row.get("virtual_number") or "").strip()
            if not virtual:
                continue

            tenant_id = (row.get("tenant_id") or "").strip() or virtual
            tenant_name = (row.get("tenant_name") or "").strip() or tenant_id

            TENANTS[virtual] = {
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "virtual_number": virtual,
                "retell_agent_id": (row.get("retell_agent_id") or "").strip(),
                "opening_line": (row.get("opening_line") or "").strip(),
            }

    print(f"✅ {len(TENANTS)} tenants geladen", flush=True)
    print(f"🔑 receivers: {list(TENANTS.keys())}", flush=True)


def get_tenant_by_receiver(receiver: str) -> Optional[Dict[str, Any]]:
    if not receiver:
        return None
    return TENANTS.get(receiver.strip())


# -----------------------------
# BILLING HELPERS
# -----------------------------
def month_key() -> str:
    return datetime.utcnow().strftime("%Y-%m")


def bump_monthly_outbound(tenant_id: str) -> int:
    """
    +1 outbound voor deze tenant in deze maand.
    Return: nieuwe maandtotal.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ontbreekt")

    mkey = month_key()

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO monthly_usage (month, tenant_id, outbound_count)
                VALUES (%s, %s, 1)
                ON CONFLICT (month, tenant_id)
                DO UPDATE SET outbound_count = monthly_usage.outbound_count + 1,
                              updated_at = NOW()
                """,
                (mkey, tenant_id),
            )
            cur.execute(
                "SELECT outbound_count FROM monthly_usage WHERE month=%s AND tenant_id=%s",
                (mkey, tenant_id),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0


# -----------------------------
# SMS VERSTUREN (sender = tenant virtual number)
# -----------------------------
def send_sms(tenant: Dict[str, Any], to_number: str, message: str) -> None:
    if not SMSTOOLS_CLIENT_ID or not SMSTOOLS_CLIENT_SECRET:
        print("❌ SMSTOOLS_CLIENT_ID/SMSTOOLS_CLIENT_SECRET ontbreken", flush=True)
        return

    if not to_number:
        print("❌ send_sms: to_number ontbreekt", flush=True)
        return

    payload = {
        "message": message,
        "to": to_number,
        "sender": tenant["virtual_number"],  # ✅ juiste fix
    }
    headers = {
        "X-Client-Id": SMSTOOLS_CLIENT_ID,
        "X-Client-Secret": SMSTOOLS_CLIENT_SECRET,
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers, timeout=15)
        print(
            f"➡️ SMS van {tenant['virtual_number']} naar {to_number} | status={response.status_code} | body={response.text}",
            flush=True,
        )
    except Exception as e:
        print(f"❌ Smstools send failed: {e}", flush=True)
        return

    # Billing nooit de SMS-flow laten blokkeren
    try:
        total = bump_monthly_outbound(tenant["tenant_id"])
        print(f"📊 Usage tenant={tenant['tenant_id']} month={month_key()} total={total}", flush=True)
    except Exception as e:
        print(f"⚠️ Billing faalde: {e}", flush=True)


# -----------------------------
# RETELL
# -----------------------------
def get_or_create_chat_id(tenant: Dict[str, Any], phone: str) -> Optional[str]:
    if not RETELL_API_KEY:
        print("⚠️ RETELL_API_KEY ontbreekt", flush=True)
        return None

    key = (tenant["tenant_id"], phone)
    if key in SMS_SESSIONS:
        return SMS_SESSIONS[key]

    try:
        resp = requests.post(
            f"{RETELL_BASE_URL}/create-chat",
            headers={
                "Authorization": f"Bearer {RETELL_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "agent_id": tenant.get("retell_agent_id"),
                "metadata": {"phone": phone, "tenant_id": tenant["tenant_id"]},
            },
            timeout=15,
        )
        print(f"🤖 Retell create-chat status={resp.status_code} body={resp.text}", flush=True)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"❌ Retell create-chat error: {e}", flush=True)
        return None

    chat_id = data.get("chat_id") or data.get("id")
    if chat_id:
        SMS_SESSIONS[key] = chat_id
    return chat_id


def ask_retell_via_sms(tenant: Dict[str, Any], phone: str, text: str) -> str:
    opening = tenant.get("opening_line") or "Bedankt voor je bericht."
    if not tenant.get("retell_agent_id") or not RETELL_API_KEY:
        return opening

    chat_id = get_or_create_chat_id(tenant, phone)
    if not chat_id:
        return opening

    try:
        resp = requests.post(
            f"{RETELL_BASE_URL}/create-chat-completion",
            headers={
                "Authorization": f"Bearer {RETELL_API_KEY}",
                "Content-Type": "application/json",
            },
            json={"chat_id": chat_id, "content": text},
            timeout=30,
        )
        print(f"🤖 Retell completion status={resp.status_code} body={resp.text}", flush=True)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"❌ Retell completion error: {e}", flush=True)
        return opening

    for m in reversed(data.get("messages", [])):
        if m.get("role") == "agent" and m.get("content"):
            return (m.get("content") or "").strip()

    return opening


# -----------------------------
# ROUTES
# -----------------------------
@app.route("/", methods=["GET"])
def health():
    return "OK", 200


@app.route("/sms/inbound", methods=["POST"])
def sms_inbound():
    # extra expliciet log, zodat je zeker bent dat dit endpoint geraakt wordt
    print("✅ /sms/inbound HIT", flush=True)

    data = request.get_json(force=True, silent=True)
    print("📥 RAW JSON:", data, flush=True)

    if not data:
        return "No JSON", 200

    event = data[0] if isinstance(data, list) else data
    webhook_type = (event.get("webhook_type") or "").strip()
    msg = event.get("message", {}) or {}

    receiver = (msg.get("receiver") or "").strip()
    tenant = get_tenant_by_receiver(receiver)

    if not tenant:
        print(f"❌ Onbekende tenant receiver={receiver}", flush=True)
        return "Onbekende tenant", 200

    print(f"🏷️ tenant={tenant['tenant_id']} receiver={receiver} webhook_type={webhook_type}", flush=True)

    # --- SMS inbox
    if webhook_type == "inbox_message":
        from_number = (msg.get("sender") or "").strip()
        text = (msg.get("content") or "").strip()

        print(f"💬 SMS van {from_number} -> {receiver}: {text}", flush=True)

        if not from_number or not text:
            return "Invalid SMS", 200

        answer = ask_retell_via_sms(tenant, from_number, text)
        send_sms(tenant, from_number, answer)
        return "OK", 200

    # --- Call events (als Smstools die ooit stuurt)
    # we maken dit expres ruim zodat we het zien in logs, ook als type afwijkt
    if "call" in webhook_type.lower():
        caller = (
            (msg.get("sender") or "")
            or (msg.get("caller") or "")
            or (msg.get("from") or "")
        ).strip()

        print(f"📞 CALL event type={webhook_type} caller={caller} msg_keys={list(msg.keys())}", flush=True)

        if caller:
            reply = tenant.get("opening_line") or "Stuur ons gerust een sms met je vraag."
            send_sms(tenant, caller, reply)

        return "OK", 200

    print(f"ℹ️ Onbekend webhook_type={webhook_type}", flush=True)
    return "OK", 200


# -----------------------------
# STARTUP
# -----------------------------
load_tenants_from_csv()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
