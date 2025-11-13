from flask import Flask, request
import requests
import os
import sys

app = Flask(__name__)

# -----------------------------
#  Config uit environment
# -----------------------------
SMSTOOLS_CLIENT_ID = os.environ.get("SMSTOOLS_CLIENT_ID")
SMSTOOLS_CLIENT_SECRET = os.environ.get("SMSTOOLS_CLIENT_SECRET")

# Retell
RETELL_API_KEY = os.environ.get("RETELL_API_KEY")      # jouw secret key uit Retell
RETELL_AGENT_ID = os.environ.get("RETELL_AGENT_ID")    # bv. agent_41c11a24d9c427ef834e778a33

# LET OP: deze URL MOET je zelf invullen volgens de officiële Retell-documentatie.
# Ik kan hun exacte endpoint niet betrouwbaar uit de zoekresultaten halen.
RETELL_TEXT_API_URL = os.environ.get(
    "RETELL_TEXT_API_URL",
    "https://api.retellai.com/…VUL_HIER_HET_JUISTE_PAD_IN…"
)

SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"


# -----------------------------
#  Helper: SMS versturen
# -----------------------------
def send_sms(to_number: str, message: str):
    """
    Stuurt een SMS via Smstools / SMSGatewayAPI.
    GEEN 'sender' meesturen => dan gebruikt Smstools je virtueel nummer.
    """
    payload = {
        "message": message,
        "to": to_number,
    }
    headers = {
        "X-Client-Id": SMSTOOLS_CLIENT_ID,
        "X-Client-Secret": SMSTOOLS_CLIENT_SECRET,
        "Content-Type": "application/json",
    }
    resp = requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers)
    print("📤 SMS verstuurd, response:", resp.text, file=sys.stdout, flush=True)


# -----------------------------
#  Helper: vraag Retell om een antwoord
# -----------------------------
def get_retell_reply(user_message: str, phone_number: str) -> str:
    """
    Stuurt de inkomende SMS naar Retell en geeft de tekstuele reply terug.
    De exacte JSON-vorm & endpoint moeten worden afgestemd op de Retell-API docs.
    Daarom is dit bewust generiek gehouden.
    """

    fallback_reply = (
        f"Bedankt voor je bericht: '{user_message}'. "
        f"We nemen zo snel mogelijk contact op. – Bel Wise"
    )

    # Als Retell nog niet geconfigureerd is, gewoon fallback gebruiken.
    if not (RETELL_API_KEY and RETELL_AGENT_ID and RETELL_TEXT_API_URL):
        print("⚠️ Retell niet volledig geconfigureerd, gebruik fallback.", flush=True)
        return fallback_reply

    try:
        payload = {
            "agent_id": RETELL_AGENT_ID,
            # Dit is een GENERIEK voorbeeld. Check in Retell-docs
            # welke velden exact nodig zijn.
            "messages": [
                {"role": "user", "content": user_message}
            ],
            "metadata": {
                "channel": "sms",
                "phone": phone_number,
            },
        }

        headers = {
            "Authorization": f"Bearer {RETELL_API_KEY}",
            "Content-Type": "application/json",
        }

        resp = requests.post(
            RETELL_TEXT_API_URL,
            json=payload,
            headers=headers,
            timeout=15,
        )

        print("🔁 Retell response:", resp.status_code, resp.text, flush=True)

        data = resp.json()

        # De volgende regels zijn ook generiek omdat ik de exacte structuur
        # van het antwoord niet mag gokken. Pas dit aan volgens de docs.
        reply = (
            data.get("reply")
            or data.get("content")
            or data.get("message")
            or ""
        ).strip()

        if not reply:
            print("⚠️ Geen bruikbare reply gevonden in Retell-antwoord, gebruik fallback.", flush=True)
            return fallback_reply

        return reply

    except Exception as e:
        print("❌ Fout bij aanroepen Retell:", repr(e), flush=True)
        return fallback_reply


# -----------------------------
#  Health check
# -----------------------------
@app.route("/", methods=["GET"])
def health():
    return "OK", 200


# -----------------------------
#  Hoofd-webhook voor Smstools
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

    # -----------------------
    # INKOMENDE SMS
    # -----------------------
    if webhook_type == "inbox_message":
        from_number = msg.get("sender")
        to_number = msg.get("receiver")
        text = (msg.get("content") or "").strip()

        print(f"📩 SMS van {from_number} naar {to_number}: {text}", flush=True)

        if from_number and text:
            # Vraag Retell om een antwoord (of fallback)
            reply_text = get_retell_reply(text, from_number)
            send_sms(from_number, reply_text)

        return "SMS verwerkt", 200

    # -----------------------
    # CALL FORWARDING
    # -----------------------
    if webhook_type == "call_forwarding":
        caller = msg.get("sender")
        receiver = msg.get("receiver")
        print(
            f"📞 Inkomende oproep van {caller} naar {receiver}",
            flush=True,
        )

        if caller:
            call_reply = (
                "Bedankt om contact op te nemen met Eleganza. "
                "Ik ben de virtuele assistent. Wat kan ik voor u doen?"
            )
            send_sms(caller, call_reply)

        return "Call verwerkt", 200

    # -----------------------
    # Onbekend type
    # -----------------------
    print(f"❓ Onbekend webhook type: {webhook_type}", flush=True)
    return "Onbekend type", 200


# Alleen lokaal relevant; op Render start gunicorn
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)



