from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

SECRET_KEY = os.getenv("SECRET_KEY")
ELEGANZA_KEY = os.getenv("ELEGANZA_KEY")
RETELL_API_KEY = os.getenv("RETELL_API_KEY")
RETELL_AGENT_ID = os.getenv("RETELL_AGENT_ID")

# -------------------------
# 1. Inkomende BelWise Webhook
# -------------------------
@app.route("/sms/inbound", methods=["POST"])
def inbound():
    data = request.json
    print("📥 Ontvangen data:", data)

    # Validate secret
    if data.get("secret") != SECRET_KEY:
        return jsonify({"error": "Invalid secret"}), 403

    msg = data.get("message", {})
    caller = msg.get("sender")          # wie belt
    receiver = msg.get("receiver")      # jouw nummer
    content = msg.get("content", "")

    print(f"📞 Inkomende oproep van {caller} naar {receiver}")

    # -------------------------
    # 2. SMS terugsturen via Eleganza
    # -------------------------
    sms_body = f"Hallo! Uw oproep is ontvangen. Wat kan ik voor u doen?"

    sms_response = requests.post(
        "https://api.eleganza.be/sms/send",
        headers={"Authorization": f"Bearer {ELEGANZA_KEY}"},
        json={
            "to": caller,
            "message": sms_body,
            "from": "Eleganza"  # max 11 chars!
        }
    )

    print("➡️ SMS verstuurd:", sms_response.text)

    return jsonify({"status": "ok"})


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




