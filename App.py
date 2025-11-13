from flask import Flask, request
import requests
import os
import sys

app = Flask(__name__)

SMSTOOLS_CLIENT_ID = os.environ.get("SMSTOOLS_CLIENT_ID")
SMSTOOLS_CLIENT_SECRET = os.environ.get("SMSTOOLS_CLIENT_SECRET")
SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"


def send_sms(to_number, message):
    """Stuur een SMS via Smstools."""
    payload = {
        "message": message,
        "to": to_number,      # GEEN 'sender' => dan gebruikt Smstools je nummer
    }
    headers = {
        "X-Client-Id": SMSTOOLS_CLIENT_ID,
        "X-Client-Secret": SMSTOOLS_CLIENT_SECRET,
        "Content-Type": "application/json",
    }

    response = requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers)
    print(f"➡️ SMS verstuurd naar {to_number}, response: {response.text}",
          file=sys.stdout, flush=True)


@app.route("/", methods=["GET"])
def health():
    return "OK", 200


@app.route("/sms/inbound", methods=["POST"])
def sms_inbound():
    data = request.get_json(force=True)
    print("📥 Ontvangen data:", data, file=sys.stdout, flush=True)

    # Soms is data een lijst, soms een dict
    if isinstance(data, list):
        event = data[0]
    else:
        event = data or {}

    webhook_type = event.get("webhook_type")
    msg = event.get("message", {})

    # ---------------------------
    # 1️⃣ INKOMENDE SMS
    # ---------------------------
    if webhook_type == "inbox_message":
        from_number = msg.get("sender")
        to_number = msg.get("receiver")
        text = (msg.get("content") or "").strip()

        print(f"📩 SMS van {from_number} naar {to_number}: {text}",
              file=sys.stdout, flush=True)

        if from_number and text:
            antwoord = (
                f"Bedankt voor je bericht: '{text}'. "
                f"We nemen zo snel mogelijk contact op. - Bel Wise"
            )
            send_sms(from_number, antwoord)

        return "SMS verwerkt", 200

    # ---------------------------
    # 2️⃣ INKOMENDE CALL (CALL FORWARDING)
    # ---------------------------
    # LET OP: Smstools stuurt 'call_forwarding'
    if webhook_type in ["call_forwarding", "call_forward", "incoming_call"]:
        caller = msg.get("sender")
        to_number = msg.get("receiver")

        print(f"📞 Inkomende oproep van {caller} naar {to_number}",
              file=sys.stdout, flush=True)

        if caller:
            call_reply = (
                "Bedankt om contact op te nemen met Eleganza. "
                "Ik ben de virtuele assistent. Wat kan ik voor u doen?"
            )
            send_sms(caller, call_reply)

        return "Call verwerkt", 200

    # ---------------------------
    # Onbekend type
    # ---------------------------
    print(f"❓ Onbekend webhook type: {webhook_type}",
          file=sys.stdout, flush=True)
    return "Onbekend type", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

