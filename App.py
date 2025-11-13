from flask import Flask, request, jsonify
import requests
import os
import sys

app = Flask(__name__)

# =========================================
#  SMS Tools (BelWise / smsgatewayapi) ENV
# =========================================
SMSTOOLS_CLIENT_ID = os.environ.get("SMSTOOLS_CLIENT_ID")
SMSTOOLS_CLIENT_SECRET = os.environ.get("SMSTOOLS_CLIENT_SECRET")

# Dit MOET jouw virtueel nummer zijn
SENDER_NUMBER = os.environ.get("SMSTOOLS_SENDER_NUMBER", "32460260667")

SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"

# =========================================
#  Retell AI ENV
# =========================================
RETELL_API_KEY = os.environ.get("RETELL_API_KEY")
RETELL_AGENT_ID = os.environ.get("RETELL_AGENT_ID")          # bv agent_xxxxx
RETELL_FROM_NUMBER = os.environ.get("RETELL_FROM_NUMBER")    # jouw Retell nummer, bv "+1202..."
ENABLE_RETELL_CALLBACK = os.environ.get("ENABLE_RETELL_CALLBACK", "0")

# =========================================
#  SMS versturen via SMS Tools
# =========================================
def send_sms(to_number, message):
    payload = {
        "message": message,
        "to": to_number,
        "sender": SENDER_NUMBER,  # JOUW nummer als afzender (vereist!)
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

# =========================================
#  Retell outbound call helper
# =========================================
def start_retell_call(to_number):
    """
    Start een outbound call via Retell AI naar 'to_number'
    """
    if not (RETELL_API_KEY and RETELL_AGENT_ID and RETELL_FROM_NUMBER):
        print("⚠️ Retell config ontbreekt, call niet gestart", flush=True)
        return

    headers = {
        "Authorization": f"Bearer {RETELL_API_KEY}",
        "Content-Type": "application/json",
    }

    body = {
        "from_number": RETELL_FROM_NUMBER,
        "to_number": to_number,
        "override_agent_id": RETELL_AGENT_ID,
    }

    resp = requests.post(
        "https://api.retellai.com/v2/create-phone-call",
        json=body,
        headers=headers,
        timeout=15,
    )

    print(f"📞 Retell call response: {resp.status_code} {resp.text}", flush=True)

# =========================================
#  HEALTH CHECK
# =========================================
@app.route("/", methods=["GET"])
def health():
    return "OK", 200

# =========================================
#  INBOUND WEBHOOK ENDPOINT (SMS Tools)
# =========================================
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

        # SMS terug
        if caller:
            call_reply = (
                "Bedankt om contact op te nemen met Eleganza. "
                "Ik ben de virtuele assistent. Wat kan ik voor u doen?"
            )
            send_sms(caller, call_reply)

            # Optioneel: Retell AI belt automatisch terug
            if ENABLE_RETELL_CALLBACK == "1":
                print("🚀 Retell callback geactiveerd, call wordt gestart...", flush=True)
                start_retell_call(caller)

        return "Call verwerkt", 200

    # -----------------------------
    # ONBEKEND TYPE
    # -----------------------------
    print(f"❓ Onbekend webhook type: {webhook_type}", flush=True)
    return "Onbekend type", 200

# =========================================
#  API om handmatig een Retell call te starten
# =========================================
@app.route("/call/start", methods=["POST"])
def call_start():
    """
    Body JSON:
    { "to": "32456776180" }   # zonder +, of met +, afhankelijk van jouw Retell config
    """
    body = request.get_json(force=True)
    to_number = body.get("to")

    if not to_number:
        return jsonify({"error": "to is required"}), 400

    start_retell_call(to_number)
    return jsonify({"status": "started", "to": to_number})

# =========================================
#  RUN FLASK LOCALLY
# =========================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
