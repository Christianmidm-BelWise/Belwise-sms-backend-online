from flask import Flask, request
import requests
import os
import sys
from typing import Dict, Set

app = Flask(__name__)

# ==========================
# ENV VARS
# ==========================
SMSTOOLS_CLIENT_ID = os.environ.get("SMSTOOLS_CLIENT_ID")
SMSTOOLS_CLIENT_SECRET = os.environ.get("SMSTOOLS_CLIENT_SECRET")
SENDER_NUMBER = os.environ.get("SMSTOOLS_SENDER_NUMBER", "32460260667")

# Retell
RETELL_API_KEY = os.environ.get("RETELL_API_KEY")
RETELL_AGENT_ID = os.environ.get("RETELL_AGENT_ID")  # bv. agent_41c11a24d9c427ef834e778a33

RETELL_BASE_URL = "https://api.retellai.com"
RETELL_CREATE_CHAT_URL = f"{RETELL_BASE_URL}/create-chat"
RETELL_CHAT_COMPLETION_URL = f"{RETELL_BASE_URL}/create-chat-completion"

SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"

# Per telefoonnummer houden we 1 Retell-chat bij
active_chats: Dict[str, str] = {}

# Nummers waarvan het gesprek gestart is via een call
started_by_call: Set[str] = set()


# ==========================
# SMS VERSTUREN
# ==========================
def send_sms(to_number: str, message: str):
    payload = {
        "message": message,
        "to": to_number,
        "sender": SENDER_NUMBER,  # Jouw virtueel nummer
    }
    headers = {
        "X-Client-Id": SMSTOOLS_CLIENT_ID,
        "X-Client-Secret": SMSTOOLS_CLIENT_SECRET,
        "Content-Type": "application/json",
    }

    resp = requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers, timeout=10)
    print(f"➡️ SMS verstuurd naar {to_number}, response: {resp.text}",
          file=sys.stdout, flush=True)


# ==========================
# RETELL HULPFUNCTIES
# ==========================
def get_or_create_chat_id(user_key: str) -> str | None:
    """Zoek bestaande chat voor dit nummer, of maak een nieuwe bij Retell."""
    if not (RETELL_API_KEY and RETELL_AGENT_ID):
        print("⚠️ RETELL_API_KEY of RETELL_AGENT_ID niet ingesteld; gebruik fallback.",
              flush=True)
        return None

    if user_key in active_chats:
        return active_chats[user_key]

    # Nieuwe chat aanmaken
    headers = {
        "Authorization": f"Bearer {RETELL_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {"agent_id": RETELL_AGENT_ID}

    try:
        resp = requests.post(
            RETELL_CREATE_CHAT_URL, json=payload, headers=headers, timeout=10
        )
        data = resp.json()
        chat_id = data.get("chat_id")
        print(f"🆕 Retell chat aangemaakt voor {user_key}: {chat_id} | raw={data}",
              flush=True)
        if chat_id:
            active_chats[user_key] = chat_id
        return chat_id
    except Exception as e:
        print("❌ Fout bij create-chat:", e, flush=True)
        return None


def ask_retell(user_key: str, user_message: str) -> str | None:
    """Stuur een bericht naar Retell en krijg een antwoordtekst terug."""
    chat_id = get_or_create_chat_id(user_key)
    if not chat_id:
        return None

    headers = {
        "Authorization": f"Bearer {RETELL_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "chat_id": chat_id,
        "content": user_message,
    }

    try:
        resp = requests.post(
            RETELL_CHAT_COMPLETION_URL, json=payload, headers=headers, timeout=15
        )
        data = resp.json()
        print(f"🤖 Retell response voor {user_key}: {data}", flush=True)

        messages = data.get("messages") or []
        for m in messages:
            if m.get("role") == "agent":
                return m.get("content")

        if messages:
            return messages[0].get("content") or str(messages[0])

        return None
    except Exception as e:
        print("❌ Fout bij create-chat-completion:", e, flush=True)
        return None


# ==========================
# HEALTH CHECK
# ==========================
@app.route("/", methods=["GET"])
def health():
    return "OK", 200


# ==========================
# INBOUND WEBHOOK
# ==========================
@app.route("/sms/inbound", methods=["POST"])
def sms_inbound():
    data = request.get_json(force=True)
    print("📥 Ontvangen data:", data, file=sys.stdout, flush=True)

    event = data[0] if isinstance(data, list) else data

    webhook_type = event.get("webhook_type")
    msg = event.get("message", {}) or {}

    # ---------- INKOMENDE SMS ----------
    if webhook_type == "inbox_message":
        from_number = msg.get("sender")
        to_number = msg.get("receiver")
        text = (msg.get("content") or "").strip()

        print(f"💬 SMS van {from_number} naar {to_number}: {text}", flush=True)

        if not (from_number and text):
            return "Geen content", 200

        # Is dit een vervolg op een call-bericht?
        if from_number in started_by_call:
            retell_prompt = (
                f"De klant reageert nu per SMS op je eerdere welkomstbericht "
                f"na een gemiste oproep. De klant zegt: '{text}'. "
                f"Beantwoord de vraag direct en stel jezelf niet opnieuw voor. "
                f"Gebruik een vriendelijke, professionele toon."
            )
            # vlag verwijderen na eerste SMS-response
            started_by_call.discard(from_number)
        else:
            # Normale conversatie via SMS
            retell_prompt = text

        reply = ask_retell(user_key=from_number, user_message=retell_prompt)

        if not reply:
            reply = (
                f"Bedankt voor je bericht: '{text}'. "
                f"We nemen zo snel mogelijk contact op. – Eleganza"
            )

        send_sms(from_number, reply)
        return "SMS conversatie verwerkt", 200

    # ---------- INKOMENDE CALL ----------
    if webhook_type in ["call_forward", "callforward", "call_forwarding", "incoming_call"]:
        caller = msg.get("sender")
        receiver = msg.get("receiver")

        print(f"📞 Inkomende oproep van {caller} naar {receiver}", flush=True)

        if not caller:
            return "Geen caller", 200

        # Zorg dat Retell weet dat dit nummer via een call gestart is
        started_by_call.add(caller)

        # Laat Retell één eerste welkomst-SMS schrijven
        system_msg = (
            "Een klant heeft net telefonisch contact proberen opnemen met "
            "kapsalon Eleganza. Stel één vriendelijke SMS op in het Nederlands, "
            "waarin je je kort voorstelt als virtuele assistent en zegt dat de klant "
            "via SMS kan antwoorden met zijn/haar vraag of afspraakverzoek."
        )

        reply = ask_retell(user_key=caller, user_message=system_msg)

        if not reply:
            reply = (
                "Bedankt om contact op te nemen met Eleganza. "
                "Je kan in deze SMS-conversatie je vraag of afspraak doorsturen, "
                "dan help ik je graag verder."
            )

        send_sms(caller, reply)
        return "Call event verwerkt", 200

    # ---------- ONBEKEND TYPE ----------
    print(f"❓ Onbekend webhook type: {webhook_type}", flush=True)
    return "Onbekend type", 200


# ==========================
# LOCAL RUN
# ==========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)


