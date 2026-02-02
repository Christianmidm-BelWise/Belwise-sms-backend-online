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
SMS_SESSIONS: Dict[tuple, str] = {}


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
            return int(cur.fetchone()[0])


# -----------------------------
# SMS VERSTUREN (FIX)
# -----------------------------
def send_sms(tenant: Dict[str, Any], to_number: str, message: str) -> None:
    payload = {
        "message": message,
        "to": to_number,
        "sender": tenant["virtual_number"],  # 🔥 JUISTE FIX
    }
    headers = {
        "X-Client-Id": SMSTOOLS_CLIENT_ID,
        "X-Client-Secret": SMSTOOLS_CLIENT_SECRET,
        "Content-Type": "application/json",
    }

    response = requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers)
    print(f"➡️ SMS van {tenant['virtual_number']} naar {to_number}: {response.text}", flush=True)

    try:
        total = bump_monthly_outbound(tenant["tenant_id"])
        print(
            f"📊 Usage {tenant['tenant_id']} {month_key()} = {total}",
            flush=True,
        )
    except Exception as e:
        print(f"⚠️ Billing faalde: {e}", flush=True)


# -----------------------------
# RETELL
# -----------------------------
def get_or_create_chat_id(tenant: Dict[str, Any], phone: str) -> Optional[str]:
    key = (tenant["tenant_id"], phone)
    if key in SMS_SESSIONS:
        return SMS_SESSIONS[key]

    resp = requests.post(
        f"{RETELL_BASE_URL}/create-chat",
        headers={
            "Authorization": f"Bearer {RETELL_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "agent_id": tenant["retell_agent_id"],
            "metadata": {"phone": phone},
        },
        timeout=10,
    )
    data = resp.json()
    chat_id = data.get("chat_id") or data.get("id")
    if chat_id:
        SMS_SESSIONS[key] = chat_id
    return chat_id


def ask_retell_via_sms(tenant: Dict[str, Any], phone: str, text: str) -> str:
    chat_id = get_or_create_chat_id(tenant, phone)
    if not chat_id:
        return tenant.get("opening_line", "Er ging iets mis.")

    resp = requests.post(
        f"{RETELL_BASE_URL}/create-chat-completion",
        headers={
            "Authorization": f"Bearer {RETELL_API_KEY}",
            "Content-Type": "application/json",
        },
        json={"chat_id": chat_id, "content": text},
        timeout=15,
    )
    data = resp.json()
    for m in reversed(data.get("messages", [])):
        if m.get("role") == "agent":
            return m.get("content", "")
    return tenant.get("opening_line", "Kan je dat anders formuleren?")


# -----------------------------
# ROUTES
# -----------------------------
@app.route("/", methods=["GET"])
def health():
    return "OK", 200


@app.route("/sms/inbound", methods=["POST"])
def sms_inbound():
    data = request.get_json(force=True)
    event = data[0] if isinstance(data, list) else data

    msg = event.get("message", {})
    receiver = msg.get("receiver")
    tenant = get_tenant_by_receiver(receiver)

    if not tenant:
        return "Onbekende tenant", 200

    from_number = msg.get("sender")
    text = (msg.get("content") or "").strip()

    if event.get("webhook_type") == "inbox_message":
        if tenant.get("retell_agent_id"):
            answer = ask_retell_via_sms(tenant, from_number, text)
        else:
            answer = tenant.get("opening_line", "Bedankt voor je bericht.")
        send_sms(tenant, from_number, answer)

    elif event.get("webhook_type") in ["call_forward", "incoming_call"]:
        reply = tenant.get("opening_line", "Stuur ons gerust een sms.")
        send_sms(tenant, msg.get("sender"), reply)

    return "OK", 200


# -----------------------------
# STARTUP
# -----------------------------
load_tenants_from_csv()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

