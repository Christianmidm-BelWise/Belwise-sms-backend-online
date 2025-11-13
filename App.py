from flask import Flask, request
import requests
import os
import sys

app = Flask(__name__)

SMSTOOLS_CLIENT_ID = os.environ.get("SMSTOOLS_CLIENT_ID")
SMSTOOLS_CLIENT_SECRET = os.environ.get("SMSTOOLS_CLIENT_SECRET")
SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"


def send_sms(to_number, message):
    payload = {"message": message, "to": to_number, "sender": "BelWise"}
    headers = {
        "X-Client-Id": SMSTOOLS_CLIENT_ID,
        "X-Client-Secret": SMSTOOLS_CLIENT_SECRET,
        "Content-Type": "application/json",
    }
    response = requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers)
    print("SMS verstuurd, response:", response.text, file=sys.stdout, flush=True)


@app.route("/", methods=["GET"])
def health():
    return "OK", 200


@app.route("/sms/inbound", methods=["POST"])
def sms_inbound():
    # JSON van SmsTools is een LIJST met één object
    data = request.get_json(force=True)
    print("Ontvangen data:", data, file=sys.stdout, flush=True)

    event = data[0]           # eerste (enige) event
    msg = event["message"]

    from_number = msg["sender"]
    to_number = msg["receiver"]
    text = (msg["content"] or "").strip()

    print(
        f"SMS van {from_number} naar {to_number}: {text}",
        file=sys.stdout,
        flush=True
    )

    # Simpele automatische reply
    if from_number and text:
        antwoord = (
            f"Bedankt voor je bericht: '{text}'. "
            f"We nemen zo snel mogelijk contact op. – Bel Wise"
        )
        send_sms(from_number, antwoord)

    return "Ontvangen", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
