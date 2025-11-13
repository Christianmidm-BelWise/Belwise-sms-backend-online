from flask import Flask, request
import requests
import os
import sys

app = Flask(__name__)

# -----------------------------
# ENV VARS (Render)
# -----------------------------
SMSTOOLS_CLIENT_ID = os.environ.get("SMSTOOLS_CLIENT_ID")
SMSTOOLS_CLIENT_SECRET = os.environ.get("SMSTOOLS_CLIENT_SECRET")
SENDER_NUMBER = os.environ.get("SMSTOOLS_SENDER_NUMBER", "32460260667")

RETELL_API_KEY = os.environ.get("RETELL_API_KEY")
RETELL_AGENT_ID = os.environ.get("RETELL_AGENT_ID")  # bv. agent_41c11a24d9c427ef834e778a33

SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"
RETELL_CHAT_URL = "https://api.retellai.com/v2/create-chat-completion"


# -----------------------------
# SMS versturen
# -----------------------------
def send_sms(to_number, message):
    payload = {
        "message": message,
        "to": to_number,
        "sender": SENDER_NUMBER,
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
# Retell aanroepen voor SMS
# -----------------------------
def ask_retell_via_sms(conversation_id: str, user_message: str) -> str:
    """
    conversation_id = telefoonnummer van de klant,
    zodat Retell de context van het gesprek bewaart.
    """

    if not (RETELL_API_KEY and RETELL_AGENT_ID):
        print("⚠️ RETELL_API_KEY of RETELL_AGENT_ID ontbreekt", flush=True)
        return (
            "Er ging iets mis aan onze kant. "
            "Kun je je vraag later nog eens sturen?"
        )

    headers = {
        "Authorization": f"Bearer {RETELL_API_KEY}",
        "Content-Type": "application/json",
    }

    body = {
        # BELANGRIJK: gebruik de AGENT, niet de LLM ID
        "agent_id": RETELL_AGENT_ID,
        "messages": [
            {
                "role": "user",
                "content": user_message,
            }
        ],
        # extra context voor Retell (optioneel, maar handig)
        "metadata": {
            "channel": "sms",
            "conversation_id": conversation_id,
        },
    }

    try:
        resp = requests.post(RETELL_CHAT_URL, json=body, headers=headers, timeout=20)
        print("📡 Retell response status:", resp.status_code, flush=True)
        print("📡 Retell raw response:", resp.text, flush=True)

        resp.raise_for_status()
        data = resp.json()

        # Standaard structuur: choices[0].message.content
        choices = data.get("choices", [])
        if not choices:
            return "Ik kon even geen goed antwoord genereren. Kun je je vraag herhalen?"

        assistant_msg = choices[0].get("message", {})
        content = assistant_msg.get("content")
        if not content:
            return "Ik kon even geen goed antwoord genereren. Kun je je vraag herhalen?"

        return content

    except Exception as e:
        print("❌ Fout bij aanroepen Retell:", e, flush=True)
        return "Er ging iets mis bij het verwerken van je bericht. Probeer het straks nog eens."


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
    msg = event.get("message", {})

    # -----------------------------
    # INKOMENDE SMS
    # -----------------------------
    if webhook_type == "inbox_message":
        from_number = msg.get("sender")
        to_number = msg.get("receiver")
        text = (msg.get("content") or "").strip()

        print(f"💬 SMS van {from_number} naar {to_number}: {text}", flush=True)

        if from_number and text:
            # Stuur bericht naar Retell-agent (met Cal.com integratie)
            assistant_reply = ask_retell_via_sms(from_number, text)
            send_sms(from_number, assistant_reply)

        return "SMS verwerkt", 200

    # -----------------------------
    # INKOMENDE CALL (CALL FORWARD)
    # -----------------------------
    if webhook_type in ["call_forward", "callforward", "call_forwarding", "incoming_call"]:
        caller = msg.get("sender")
        receiver = msg.get("receiver")

        print(
            f"📞 Inkomende oproep van {caller} naar {receiver}",
            flush=True
        )

        if caller:
            # Alleen een korte uitnodiging sturen; echte conversatie via SMS + Retell
            call_reply = (
                "Bedankt om te bellen met Kapperzaak Eleganza. "
                "Ik ben de virtuele assistent. Je kunt me via sms een vraag sturen "
                "of laten weten wanneer je een afspraak wilt maken."
            )
            send_sms(caller, call_reply)

        return "Call verwerkt", 200

    # -----------------------------
    # ONBEKEND TYPE
    # -----------------------------
    print(f"❓ Onbekend webhook type: {webhook_type}", flush=True)
    return "Onbekend type", 200


# -----------------------------
# RUN FLASK LOCALLY
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

