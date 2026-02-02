from flask import Flask, request, jsonify
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


# -----------------------------
# LOG ALLE REQUESTS (optioneel)
# -----------------------------
@app.before_request
def log_request():
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
        print("⚠️ tenants.csv niet gevonden", flush=True)
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


def bump_monthly_outbound(tenant_id: str) -> None:
    if not DATABASE_URL:
        return

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
                (month_key(), tenant_id),
            )


# -----------------------------
# SMS VERSTUREN
# -----------------------------
def send_sms(tenant: Dict[str, Any], to_number: str, message: str) -> None:
    if not to_number:
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
        requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers, timeout=15)
        bump_monthly_outbound(tenant["tenant_id"])
    except Exception as e:
        print(f"⚠️ SMS send error: {e}", flush=True)


# -----------------------------
# RETELL
# -----------------------------
def get_or_create_chat_id(tenant: Dict[str, Any], phone: str) -> Optional[str]:
    key = (tenant["tenant_id"], phone)
    if key in SMS_SESSIONS:
        return SMS_SESSIONS[key]

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
            timeout=10,
        )
        data = r.json()
        chat_id = data.get("chat_id") or data.get("id")
        if chat_id:
            SMS_SESSIONS[key] = chat_id
            return chat_id
    except Exception:
        pass

    return None


def ask_retell_via_sms(tenant: Dict[str, Any], phone: str, text: str) -> str:
    opening = tenant.get("opening_line") or "Bedankt voor je bericht."

    if not tenant.get("retell_agent_id"):
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
            timeout=15,
        )
        data = r.json()
        for m in reversed(data.get("messages", [])):
            if m.get("role") == "agent":
                return m.get("content", "").strip()
    except Exception:
        pass

    return opening


# -----------------------------
# ROUTES
# -----------------------------
@app.route("/", methods=["GET"])
def health():
    return "OK", 200


@app.route("/sms/inbound", methods=["POST"])
def sms_inbound():
    data = request.get_json(force=True, silent=True)
    if not data:
        return "OK", 200

    event = data[0] if isinstance(data, list) else data
    msg = event.get("message", {})
    receiver = msg.get("receiver")

    tenant = get_tenant_by_receiver(receiver)
    if not tenant:
        return "OK", 200

    if event.get("webhook_type") == "inbox_message":
        sender = msg.get("sender")
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
    if not DATABASE_URL:
        return jsonify({"data": []})

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT month, tenant_id, outbound_count
                FROM monthly_usage
                ORDER BY month DESC, tenant_id
            """)
            rows = cur.fetchall()

    return jsonify({
        "data": [
            {
                "month": month,
                "tenant_id": tenant_id,
                "outbound_count": outbound_count
            }
            for month, tenant_id, outbound_count in rows
        ]
    })


# -----------------------------
# STARTUP
# -----------------------------
load_tenants_from_csv()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

