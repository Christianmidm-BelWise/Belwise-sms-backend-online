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
    data = request.get_json(force=True)
    print("Ontvangen data:", data, file=sys.stdout, flush=True)

    from_number = data.get("from")
    message = (data.get("message") or "").lower()

    if from_number and message:
        antwoord = f"Bedankt voor je bericht: '{message}'. We nemen spoedig contact op. – Bel Wise"
        send_sms(from_number, antwoord)

    return "Ontvangen", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
