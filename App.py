from flask import Flask, request
import requests
import os
import sys
import csv
from typing import Dict, Any, Optional

app = Flask(__name__)

# -----------------------------
# ENV VARS - SMSTOOLS (GLOBAAL)
# -----------------------------
SMSTOOLS_CLIENT_ID = os.environ.get("SMSTOOLS_CLIENT_ID")
SMSTOOLS_CLIENT_SECRET = os.environ.get("SMSTOOLS_CLIENT_SECRET")
SENDER_NUMBER = os.environ.get("SMSTOOLS_SENDER_NUMBER", "32460260667")
SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"

# -----------------------------
# ENV VARS - RETELL (GLOBAAL)
# -----------------------------
RETELL_API_KEY = os.environ.get("RETELL_API_KEY")
RETELL_BASE_URL = "https://api.retellai.com"

# -----------------------------
# TENANTS (UIT CSV)
# -----------------------------
# key = virtual_number (string), value = dict met tenant-info
TENANTS: Dict[str, Dict[str, Any]] = {}

# key = (tenant_id, phone_number) -> chat_id
SMS_SESSIONS: Dict[tuple, str] = {}


def load_tenants_from_csv(path: str = "tenants.csv") -> None:
    """
    Laadt alle tenant-configs uit tenants.csv in het geheugen.
    Verwacht kolommen:
    tenant_id,tenant_name,virtual_number,retell_agent_id,opening_line
    """
    global TENANTS
    TENANTS = {}

    if not os.path.exists(path):
        print(f"⚠️ tenants.csv niet gevonden op pad: {path}", file=sys.stdout, flush=True)
        return

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            virtual_number = (row.get("virtual_number") or "").strip()
            if not virtual_number:
                print(f"⚠️ Rij zonder virtual_number overgeslagen: {row}", flush=True)
                continue

            tenant_id = (row.get("tenant_id") or "").strip() or virtual_number
            tenant_name = (row.get("tenant_name") or "").strip() or tenant_id
            agent_id = (row.get("retell_agent_id") or "").strip()
            opening_line = (row.get("opening_line") or "").strip()

            TENANTS[virtual_number] = {
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "virtual_number": virtual_number,
                "retell_agent_id": agent_id,
                "opening_line": opening_line,  # optioneel, mag leeg zijn
            }

    print(f"✅ {len(TENANTS)} tenants geladen uit {path}", file=sys.stdout, flush=True)


def get_tenant_by_receiver(receiver: str) -> Optional[Dict[str, Any]]:
    """
    Zoek de juiste tenant op basis van het 'receiver'-nummer uit de webhook.
    """
    if not receiver:
        return None
    receiver = receiver.strip()
    tenant = TENANTS.get(receiver)
    if not tenant:
        print(f"⚠️ Geen tenant gevonden voor receiver {receiver}", flush=True)
    return tenant


# -----------------------------
# SMS versturen via Smstools (globale credentials)
# -----------------------------
def send_sms(to_number: str, message: str) -> None:
    if not SMSTOOLS_CLIENT_ID or not SMSTOOLS_CLIENT_SECRET:
        print("❌ SMSTOOLS_CLIENT_ID of SMSTOOLS_CLIENT_SECRET ontbreekt", flush=True)
        return

    payload = {
        "message": message,
        "to": to_number,
        "sender": SENDER_NUMBER,  # globale afzender
    }
    headers = {
        "X-Client-Id": SMSTOOLS_CLIENT_ID,
        "X-Client-Secret": SMSTOOLS_CLIENT_SECRET,
        "Content-Type": "application/json",
    }
    response = requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers)
    print(
        f"➡️ SMS verstuurd naar {to_number}, response: {response.text}",
        file=sys.stdout,
        flush=True,
    )


# -----------------------------
# Retell helpers (multi-tenant via agent_id)
# -----------------------------
def get_or_create_chat_id(tenant: Dict[str, Any], phone_number: str) -> Optional[str]:
    """
    Haalt een bestaand chat_id op voor deze tenant + telefoonnummer,
    of maakt een nieuwe chat aan bij Retell.
    """
    if not RETELL_API_KEY:
        print("⚠️ RETELL_API_KEY ontbreekt", flush=True)
        return None

    agent_id = tenant.get("retell_agent_id")
    if not agent_id:
        print(
            f"⚠️ Tenant {tenant['tenant_id']} heeft geen retell_agent_id",
            flush=True,
        )
        return None

    session_key = (tenant["tenant_id"], phone_number)

    # Bestaat er al een sessie?
    if session_key in SMS_SESSIONS:
        return SMS_SESSIONS[session_key]

    # Nieuwe chat aanmaken
    payload = {
        "agent_id": agent_id,
        "metadata": {
            "channel": "sms",
            "phone_number": phone_number,
            "tenant_id": tenant["tenant_id"],
            "tenant_name": tenant["tenant_name"],
        },
    }
    headers = {
        "Authorization": f"Bearer {RETELL_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(
            f"{RETELL_BASE_URL}/create-chat",
            json=payload,
            headers=headers,
            timeout=10,
        )
        print(
            f"📡 [{tenant['tenant_name']}] Retell create-chat status:",
            resp.status_code,
            flush=True,
        )
        print(
            f"📡 [{tenant['tenant_name']}] Retell create-chat raw:",
            resp.text,
            flush=True,
        )
        resp.raise_for_status()
        data = resp.json()
        chat_id = data.get("chat_id") or data.get("id")
        if not chat_id:
            print("❌ Geen chat_id in create-chat response", data, flush=True)
            return None

        SMS_SESSIONS[session_key] = chat_id
        print(
            f"🧵 Nieuwe SMS-sessie voor {phone_number} (tenant {tenant['tenant_id']}): {chat_id}",
            flush=True,
        )
        return chat_id
    except Exception as e:
        print(f"❌ Fout bij create-chat: {e}", flush=True)
        return None


def ask_retell_via_sms(tenant: Dict[str, Any], phone_number: str, user_text: str) -> str:
    """
    Stuurt de SMS-vraag naar Retell en geeft het antwoord van de agent terug als string.
    """
    if not RETELL_API_KEY:
        return (
            "Er ging iets mis bij het verbinden met de virtuele assistent. "
            "Probeer het later nog eens of bel ons even."
        )

    chat_id = get_or_create_chat_id(tenant, phone_number)
    if not chat_id:
        return (
            "Er ging iets mis bij het verbinden met de virtuele assistent. "
            "Probeer het later nog eens of bel ons even."
        )

    payload = {
        "chat_id": chat_id,
        "content": user_text,
    }
    headers = {
        "Authorization": f"Bearer {RETELL_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(
            f"{RETELL_BASE_URL}/create-chat-completion",
            json=payload,
            headers=headers,
            timeout=15,
        )
        print(
            f"📡 [{tenant['tenant_name']}] Retell completion status:",
            resp.status_code,
            flush=True,
        )
        print(
            f"📡 [{tenant['tenant_name']}] Retell completion raw:",
            resp.text,
            flush=True,
        )
        resp.raise_for_status()
        data = resp.json()
        messages = data.get("messages", [])

        # Pak laatste agent-bericht
        agent_answer = None
        for m in reversed(messages):
            if m.get("role") == "agent" and m.get("content"):
                agent_answer = m["content"].strip()
                break

        if agent_answer:
            return agent_answer

        return (
            "Ik kon je vraag niet goed verwerken. "
            "Kun je het misschien anders formuleren?"
        )
    except Exception as e:
        print(f"❌ Fout bij create-chat-completion: {e}", flush=True)
        return (
            "Er ging iets mis bij het verwerken van je bericht. "
            "Probeer het straks nog eens."
        )


# -----------------------------
# HEALTH CHECK
# -----------------------------
@app.route("/", methods=["GET"])
def health():
    return "OK", 200


# -----------------------------
# INBOUND WEBHOOK ENDPOINT
# -----------------------------
@app.route("/sms/inbound", methods=["POST"])
def sms_inbound():
    data = request.get_json(force=True)
    print("📥 Ontvangen data:", data, file=sys.stdout, flush=True)

    # Soms lijst, soms dict
    if isinstance(data, list):
        event = data[0]
    else:
        event = data

    webhook_type = event.get("webhook_type")
    msg = event.get("message", {}) or {}

    receiver = msg.get("receiver")  # virtueel nummer van klant
    tenant = get_tenant_by_receiver(receiver)

    if not tenant:
        # Geen tenant gevonden: generieke fallback
        print(
            f"❌ Geen tenant-config gevonden voor receiver {receiver}, event wordt genegeerd.",
            flush=True,
        )
        return "Onbekende tenant", 200

    opening_line = tenant.get("opening_line") or ""

    # --------------------------
    # INKOMENDE SMS
    # --------------------------
    if webhook_type == "inbox_message":
        from_number = msg.get("sender")
        text = (msg.get("content") or "").strip()
        print(
            f"💬 [{tenant['tenant_name']}] SMS van {from_number} naar {receiver}: {text}",
            flush=True,
        )

        if not from_number or not text:
            return "Geen geldige SMS", 200

        # Als Retell niet geconfigureerd is (globaal of voor deze tenant), simpele fallback
        if not RETELL_API_KEY or not tenant.get("retell_agent_id"):
            if opening_line:
                fallback = opening_line
            else:
                fallback = (
                    f"Bedankt voor je bericht! De virtuele assistent van {tenant['tenant_name']} "
                    "is momenteel niet actief. We nemen zo snel mogelijk contact met je op."
                )
            send_sms(from_number, fallback)
            return "SMS verwerkt (fallback)", 200

        # Vraag doorsturen naar Retell
        antwoord = ask_retell_via_sms(tenant, from_number, text)
        send_sms(from_number, antwoord)
        return "SMS verwerkt", 200

    # --------------------------
    # INKOMENDE CALL (CALL FORWARD)
    # --------------------------
    if webhook_type in ["call_forward", "callforward", "call_forwarding", "incoming_call"]:
        caller = msg.get("sender")
        print(
            f"📞 [{tenant['tenant_name']}] Inkomende oproep van {caller} naar {receiver}",
            flush=True,
        )

        if caller:
            if opening_line:
                call_reply = opening_line
            else:
                call_reply = (
                    f"Bedankt om te bellen met {tenant['tenant_name']}. "
                    "Ik ben de virtuele assistent. "
                    "Stuur me gerust een sms met je vraag of wanneer je een afspraak wilt plannen."
                )
            send_sms(caller, call_reply)

        return "Call verwerkt", 200

    # --------------------------
    # ONBEKEND TYPE
    # --------------------------
    print(f"❓ Onbekend webhook type: {webhook_type}", flush=True)
    return "Onbekend type", 200


# -----------------------------
# STARTUP (CSV inladen)
# -----------------------------
load_tenants_from_csv()

# -----------------------------
# RUN FLASK LOCALLY
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

