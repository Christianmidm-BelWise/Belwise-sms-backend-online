from flask import Flask, request
import requests
import os
import sys

app = Flask(__name__)

SMSTOOLS_CLIENT_ID = os.environ.get("SMSTOOLS_CLIENT_ID")
SMSTOOLS_CLIENT_SECRET = os.environ.get("SMSTOOLS_CLIENT_SECRET")
SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"


def send_sms(to_number, message):
    payload = {"message": message, "to": to_number, "sender": "Eleganza"}
    headers = {
        "X-Client-Id": SMSTOOLS_CLIENT_ID,
        "X-Client-Secret": SMSTOOLS_CLIENT_SECRET,
        "Content-Type": "application/json",
    }

    response = requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers)
    print("SMS verstuurd, response:", response.text, flush=True)


@app.route("/sms/inbound", methods=["POST"])
def sms_inbound():
    data = request.get_json(force=True)
    print("Ontvangen data:", data, flush=True)

    event = data[0] if isinstance(data, list) else data
    webhook_type = event.get("webhook_type")

    # ============================
    # 📩 1. INKOMENDE SMS
    # ============================
    if webhook_type == "inbox_message":
        msg = event.get("message", {})
        sender = msg.get("sender")
        text = (msg.get("content") or "").strip()

        print(f"SMS van {sender}: {text}", flush=True)

        antwoord = (
            f"Bedankt voor je bericht: '{text}'. "
            f"We nemen zo snel mogelijk contact op. – Eleganza"
        )
        send_sms(sender, antwoord)
        return "OK", 200

    # ============================
    # 📞 2. INKOMENDE OPROEP
    # ============================
    if webhook_type == "call_forward":
        caller = event.get("caller")
        print(f"Inkomende oproep van: {caller}", flush=True)

        antwoord = (
            "Bedankt om contact op te nemen met Eleganza. "
            "Ik ben de virtuele assistent van Eleganza. "
            "Wat kan ik voor u doen?"
        )
        send_sms(caller, antwoord)
        return "OK", 200

    return "Unknown event", 200

