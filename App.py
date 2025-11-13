from flask import Flask, request
import requests
import os
import sys

app = Flask(__name__)

# -----------------------------
# ENV VARS - SMSTOOLS
# -----------------------------
SMSTOOLS_CLIENT_ID = os.environ.get("SMSTOOLS_CLIENT_ID")
SMSTOOLS_CLIENT_SECRET = os.environ.get("SMSTOOLS_CLIENT_SECRET")
SENDER_NUMBER = os.environ.get("SMSTOOLS_SENDER_NUMBER", "32460260667")
SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"

# -----------------------------
# ENV VARS - RETELL
# -----------------------------
RETELL_API_KEY = os.environ.get("RETELL_API_KEY")
RETELL_AGENT_ID = os.environ.get("RETELL_AGENT_ID")  # bv. agent_ag_...

RETELL_BASE_URL = "https://api.retellai.com"

# In-memory map: telefoonnummer -> chat_id
# (verdwijnt bij herstart, maar dat is oké voor nu)
SMS_SESSIONS = {}


# -----------------------------
# SMS versturen via Smstools
# -----------------------------
def send_sms(to_number: str, message: str) -> None:
    payload = {
        "message": message,
        "to": to_number,
        "sender": SENDER_NUMBER,  # jouw virtueel nummer
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
# Retell helpers
# -----------------------------
def get_or_create_chat_id(phone_number: str) -> str | None:
    """
    Haalt een bestaand chat_id op voor dit nummer,
    of maakt een nieuwe chat aan bij Retell.
    """
    if not RETELL_API_KEY or not RETELL_AGENT_ID:
        print("⚠️ RETELL_API_KEY of RETELL_AGENT_ID ontbreekt", flush=True)
        return None

    # Bestaat er al een sessie voor dit nummer?
    if phone_number in SMS_SESSIONS:
        return SMS_SESSIONS[phone_number]

    # Nieuwe chat aanmaken
    payload = {
        "agent_id": RETELL_AGENT_ID,
        "metadata": {
            "channel": "sms",
            "phone_number": phone_number,
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
        print("📡 Retell create-chat status:", resp.status_code, flush=True)
        print("📡 Retell create-chat raw:", resp.text, flush=True)
        resp.raise_for_status()

        data = resp.json()
        chat_id = data.get("chat_id") or data.get("id")
        if not chat_id:
            print("❌ Geen chat_id in create-chat response", data, flush=True)
            return None

        SMS_SESSIONS[phone_number] = chat_id
        print(f"🧵 Nieuwe SMS-sessie voor {phone_number}: {chat_id}", flush=True)
        return chat_id

    except Exception as e:
        print(f"❌ Fout bij create-chat: {e}", flush=True)
        return None


def ask_retell_via_sms(phone_number: str, user_text: str) -> str:
    """
    Stuurt de SMS-vraag naar Retell en geeft het antwoord
    van de agent terug als string.
    """
    chat_id = get_or_create_chat_id(phone_number)
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
        print("📡 Retell completion status:", resp.status_code, flush=True)
        print("📡 Retell completion raw:", resp.text, flush=True)
        resp.raise_for_status()

        data = resp.json()
        messages = data.get("messages", [])

        # Pak HET LAATSTE agent-bericht i.p.v. het eerste
        agent_answer = None
        for m in reversed(messages):
            if m.get("role") == "agent" and m.get("content"):
                agent_answer = m["content"].strip()
                break

        if agent_answer:
            return agent_answer

        # Fallback als er geen duidelijke agenttekst is
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

    # --------------------------
    # INKOMENDE SMS
    # --------------------------
    if webhook_type == "inbox_message":
        from_number = msg.get("sender")
        to_number = msg.get("receiver")
        text = (msg.get("content") or "").strip()

        print(f"💬 SMS van {from_number} naar {to_number}: {text}", flush=True)

        if not from_number or not text:
            return "Geen geldige SMS", 200

        # Als Retell niet geconfigureerd is, simpele fallback
        if not RETELL_API_KEY or not RETELL_AGENT_ID:
            fallback = (
                "Bedankt voor je bericht! Onze virtuele assistent is nog niet actief. "
                "We nemen zo snel mogelijk contact met je op."
            )
            send_sms(from_number, fallback)
            return "SMS verwerkt (fallback)", 200

        # Vraag doorsturen naar Retell
        antwoord = ask_retell_via_sms(from_number, text)
        send_sms(from_number, antwoord)

        return "SMS verwerkt", 200

    # --------------------------
    # INKOMENDE CALL (CALL FORWARD)
    # --------------------------
    if webhook_type in ["call_forward", "callforward", "call_forwarding", "incoming_call"]:
        caller = msg.get("sender")
        receiver = msg.get("receiver")

        print(f"📞 Inkomende oproep van {caller} naar {receiver}", flush=True)

        if caller:
            call_reply = (
                "Bedankt om te bellen met Kapperzaak Eleganza. "
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
# RUN FLASK LOCALLY
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
