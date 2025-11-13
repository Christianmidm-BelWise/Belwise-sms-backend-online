from flask import Flask, request
import requests
import os
import sys

app = Flask(__name__)

# ENV variables
SMSTOOLS_CLIENT_ID = os.environ.get("SMSTOOLS_CLIENT_ID")
SMSTOOLS_CLIENT_SECRET = os.environ.get("SMSTOOLS_CLIENT_SECRET")

# Dit MOET jouw virtueel nummer zijn
SENDER_NUMBER = os.environ.get("SMSTOOLS_SENDER_NUMBER", "32460260667")

SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"


# -----------------------------
# SMS versturen functie
# -----------------------------
def send_sms(to_number, message):
    payload = {
        "message": message,
        "to": to_number,
        "sender": SENDER_NUMBER  # JOUW nummer als afzender (vereist!)
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
            antwoord = (
                f"Bedankt voor je bericht: '{text}'. "
                f"We nemen zo snel mogelijk contact op. – Bel Wise"
            )
            send_sms(from_number, antwoord)

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
            call_reply = (
                "Bedankt om contact op te nemen met Eleganza. "
                "Ik ben de virtuele assistent. Wat kan ik voor u doen?"
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


# -------------------------
# 3. Outbound call starten via Retell
# -------------------------
@app.route("/call/start", methods=["POST"])
def start_call():
    body = request.json
    to_number = body.get("to")  # bv: "+324xxxxxxxx"

    response = requests.post(
        "https://api.retellai.com/v2/create-phone-call",
        headers={"Authorization": f"Bearer {RETELL_API_KEY}"},
        json={
            "from_number": "+12029420324",  # Dit is je Retell nummer
            "to_number": to_number,
            "override_agent_id": RETELL_AGENT_ID
        }
    )

    return jsonify(response.json())


@app.route("/", methods=["GET"])
def home():
    return "Eleganza SMS + Retell backend draait!"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)




