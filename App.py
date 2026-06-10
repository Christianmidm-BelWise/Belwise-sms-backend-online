import os
import re
import csv
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import requests
import psycopg2
from psycopg2 import errors as pg_errors
from flask import Flask, request, jsonify

app = Flask(__name__)

# =========================
# ENV / CONFIG
# =========================

SMSTOOLS_CLIENT_ID = (os.environ.get("SMSTOOLS_CLIENT_ID") or "").strip()
SMSTOOLS_CLIENT_SECRET = (os.environ.get("SMSTOOLS_CLIENT_SECRET") or "").strip()
SMSTOOLS_SEND_URL = "https://api.smsgatewayapi.com/v1/message/send"

RETELL_API_KEY = (os.environ.get("RETELL_API_KEY") or "").strip()
RETELL_BASE_URL = "https://api.retellai.com"

RAW_DATABASE_URL = os.environ.get("DATABASE_URL") or ""
TENANTS_CSV_PATH = os.environ.get("TENANTS_CSV_PATH") or "tenants.csv"
PREFERRED_TENANT_ID = (
    (os.environ.get("REACTIFY_TENANT_ID") or "").strip()
    or (os.environ.get("CURRENT_TENANT_ID") or "").strip()
    or (os.environ.get("TENANT_ID") or "").strip()
)

# ✅ Admin token (met fallbacks)
ADMIN_TOKEN = (
    (os.environ.get("ADMIN_TOKEN") or "").strip()
    or (os.environ.get("ADMIN_API_KEY") or "").strip()
    or (os.environ.get("ADMIN_SECRET") or "").strip()
)

DEBUG_LOGS = (os.environ.get("DEBUG_LOGS") or "true").lower() in ("1", "true", "yes", "y")


def log(msg: str) -> None:
    if DEBUG_LOGS:
        print(msg, flush=True)


@app.before_request
def _log_request() -> None:
    try:
        log(f"➡️ {request.method} {request.path} qs={request.query_string.decode('utf-8', 'ignore')}")
    except Exception:
        pass


def sanitize_database_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    m = re.search(r"(postgres(?:ql)?://\S+)", raw)
    return (m.group(1).strip() if m else raw.strip())


DATABASE_URL = sanitize_database_url(RAW_DATABASE_URL)


def db_available() -> bool:
    return bool(DATABASE_URL)


def month_key(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.utcnow()
    return d.strftime("%Y-%m")


def normalize_phone(raw: str) -> str:
    s = re.sub(r"\D+", "", (raw or "").strip())
    if not s:
        return ""
    if s.startswith("0032"):
        s = s[2:]
    if s.startswith("32"):
        return "+32" + s[2:]
    if s.startswith("0"):
        return "+32" + s[1:]
    if len(s) == 9 and s.startswith("4"):
        return "+32" + s
    return "+" + s if raw.strip().startswith("+") else s


def detect_csv_delimiter(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            header = f.readline()
        return ";" if header.count(";") >= header.count(",") else ","
    except Exception:
        return ";"


def to_int_safe(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        s = str(value).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def _mask_token(t: str) -> str:
    t = (t or "").strip()
    if len(t) <= 8:
        return "*" * len(t)
    return t[:4] + "..." + t[-4:]


def _extract_admin_token_from_request() -> str:
    """
    Accept token in:
      - query: ?token=...
      - header: Authorization: Bearer <token>
      - header: X-Admin-Token: <token>
      - header: X-API-Key: <token>
    """
    token = (request.args.get("token") or "").strip()
    if token:
        return token

    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()

    token = (request.headers.get("X-Admin-Token") or "").strip()
    if token:
        return token

    token = (request.headers.get("X-API-Key") or "").strip()
    if token:
        return token

    return ""


def require_admin_token() -> Optional[Any]:
    """
    If ADMIN_TOKEN is empty -> admin endpoints are OPEN (not recommended).
    """
    if not ADMIN_TOKEN:
        log("⚠️ ADMIN_TOKEN is empty -> admin endpoints are OPEN.")
        return None

    provided = _extract_admin_token_from_request()
    log(f"🔐 Admin auth check expected={_mask_token(ADMIN_TOKEN)} provided={_mask_token(provided)}")

    if not provided or provided != ADMIN_TOKEN:
        return jsonify({"error": "unauthorized"}), 401

    return None


# =========================
# TENANTS (CSV)
# =========================

TENANTS_BY_VIRTUAL: Dict[str, Dict[str, Any]] = {}
TENANTS_BY_ID: Dict[str, Dict[str, Any]] = {}
SMS_SESSIONS: Dict[Tuple[str, str], str] = {}  # (tenant_id, phone) -> chat_id


def load_tenants_from_csv(path: str) -> None:
    global TENANTS_BY_VIRTUAL, TENANTS_BY_ID
    TENANTS_BY_VIRTUAL = {}
    TENANTS_BY_ID = {}

    if not os.path.exists(path):
        log(f"⚠️ tenants.csv not found at {path}")
        return

    delimiter = detect_csv_delimiter(path)
    log(f"ℹ️ tenants.csv delimiter='{delimiter}' path={path}")

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            tenant_id = (row.get("tenant_id") or "").strip()
            virtual_raw = (row.get("virtual_number") or "").strip()
            if not tenant_id or not virtual_raw:
                continue

            tenant = {
                "tenant_id": tenant_id,
                "stripe_customer_id": (row.get("stripe_customer_id") or "").strip(),
                "company_name": (row.get("company_name") or "").strip(),
                "company_number": (row.get("company_number") or "").strip(),
                "virtual_number": virtual_raw,
                "retell_agent_id": (row.get("retell_agent_id") or "").strip(),
                "plan": (row.get("plan") or "").strip().lower(),
                "opening_line": (row.get("opening_line") or "").strip(),
            }

            TENANTS_BY_VIRTUAL[normalize_phone(virtual_raw)] = tenant
            TENANTS_BY_ID[tenant_id] = tenant

    log(f"✅ Loaded tenants: {len(TENANTS_BY_ID)}")


def get_tenant_by_receiver(receiver: str) -> Optional[Dict[str, Any]]:
    return TENANTS_BY_VIRTUAL.get(normalize_phone(receiver or ""))


def get_overage_price_eur(plan: str) -> float:
    p = (plan or "").strip().lower()
    return 0.17 if p == "advanced" else 0.19


# =========================
# DB: monthly_usage
# =========================

def ensure_monthly_usage_table() -> None:
    if not db_available():
        log("⚠️ DATABASE_URL missing; usage tracking disabled")
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS monthly_usage (
                      month TEXT NOT NULL,
                      tenant_id TEXT NOT NULL,
                      outbound_count INT NOT NULL DEFAULT 0,
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      PRIMARY KEY (month, tenant_id)
                    );
                    """
                )
        log("✅ monthly_usage ensured")
    except Exception as e:
        log(f"⚠️ ensure_monthly_usage_table failed: {e}")


def bump_monthly_outbound(tenant_id: str, amount: int = 1) -> None:
    if not db_available():
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO monthly_usage (month, tenant_id, outbound_count)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (month, tenant_id)
                    DO UPDATE SET outbound_count = monthly_usage.outbound_count + EXCLUDED.outbound_count,
                                  updated_at = NOW();
                    """,
                    (month_key(), tenant_id, int(amount)),
                )
        log(f"✅ outbound+{amount} tenant={tenant_id} month={month_key()}")
    except pg_errors.UndefinedTable:
        ensure_monthly_usage_table()
        bump_monthly_outbound(tenant_id, amount)
    except Exception as e:
        log(f"⚠️ bump_monthly_outbound failed: {e}")




# =========================
# DB: conversations + messages
# =========================

def ensure_conversation_tables() -> None:
    if not db_available():
        log("⚠️ DATABASE_URL missing; conversations disabled")
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS conversations (
                      id TEXT PRIMARY KEY,
                      tenant_id TEXT NOT NULL,
                      contact_phone TEXT,
                      contact_email TEXT,
                      contact_name TEXT,
                      channel TEXT NOT NULL DEFAULT 'sms',
                      status TEXT NOT NULL DEFAULT 'ai-active',
                      intent TEXT,
                      urgency TEXT,
                      requires_human BOOLEAN NOT NULL DEFAULT FALSE,
                      summary TEXT,
                      recommended_action TEXT,
                      suggested_reply TEXT,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );

                    CREATE TABLE IF NOT EXISTS conversation_messages (
                      id TEXT PRIMARY KEY,
                      conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
                      tenant_id TEXT NOT NULL,
                      direction TEXT NOT NULL,
                      channel TEXT NOT NULL DEFAULT 'sms',
                      body TEXT NOT NULL,
                      external_id TEXT,
                      sender_type TEXT NOT NULL DEFAULT 'unknown',
                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );

                    CREATE INDEX IF NOT EXISTS idx_conversations_tenant_updated
                      ON conversations (tenant_id, updated_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_conversations_phone
                      ON conversations (tenant_id, contact_phone);
                    ALTER TABLE conversation_messages ADD COLUMN IF NOT EXISTS sender_type TEXT NOT NULL DEFAULT 'unknown';
                    UPDATE conversation_messages SET sender_type = CASE WHEN direction = 'incoming' THEN 'customer' ELSE 'unknown' END WHERE sender_type IS NULL OR sender_type = '';
                    UPDATE conversations SET contact_phone = CASE WHEN contact_phone ~ '^32' THEN '+' || contact_phone WHEN contact_phone ~ '^0' THEN '+32' || SUBSTRING(contact_phone FROM 2) ELSE contact_phone END WHERE contact_phone IS NOT NULL AND contact_phone <> '';

                    CREATE INDEX IF NOT EXISTS idx_messages_conversation_created
                      ON conversation_messages (conversation_id, created_at ASC);
                    """
                )
        log("✅ conversation tables ensured")
    except Exception as e:
        log(f"⚠️ ensure_conversation_tables failed: {e}")


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def get_default_tenant() -> Optional[Dict[str, Any]]:
    # Prefer an explicit current tenant so old tenants/bots never leak into the platform.
    if PREFERRED_TENANT_ID and PREFERRED_TENANT_ID in TENANTS_BY_ID:
        return TENANTS_BY_ID[PREFERRED_TENANT_ID]
    # If there is only one tenant in tenants.csv, using it is safe.
    if len(TENANTS_BY_ID) == 1:
        return next(iter(TENANTS_BY_ID.values()))
    # With multiple tenants, do NOT silently pick the first one. This caused old bot contacts to appear.
    log("⚠️ Multiple tenants found but REACTIFY_TENANT_ID/CURRENT_TENANT_ID/TENANT_ID is missing or invalid.")
    return None


def get_tenant_from_request_or_default() -> Optional[Dict[str, Any]]:
    tenant_id = (
        request.args.get("tenant_id")
        or request.args.get("tenantId")
        or request.headers.get("X-Tenant-Id")
        or ""
    ).strip()
    if tenant_id and tenant_id in TENANTS_BY_ID:
        return TENANTS_BY_ID[tenant_id]
    return get_default_tenant()


def get_or_create_conversation(tenant: Dict[str, Any], phone: str = "", name: str = "", email: str = "", channel: str = "sms") -> Optional[Dict[str, Any]]:
    if not db_available() or not tenant:
        return None
    normalized_phone = normalize_phone(phone) if phone else ""
    tenant_id = tenant["tenant_id"]
    try:
        ensure_conversation_tables()
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                existing = None
                if normalized_phone:
                    cur.execute(
                        """
                        SELECT id, status FROM conversations
                        WHERE tenant_id = %s AND contact_phone = %s
                        ORDER BY updated_at DESC
                        LIMIT 1;
                        """,
                        (tenant_id, normalized_phone),
                    )
                    existing = cur.fetchone()
                if existing:
                    conv_id = existing[0]
                    existing_status = existing[1] if len(existing) > 1 else 'ai-active'
                    cur.execute(
                        """
                        UPDATE conversations
                        SET contact_name = COALESCE(NULLIF(%s, ''), contact_name),
                            contact_email = COALESCE(NULLIF(%s, ''), contact_email),
                            channel = COALESCE(NULLIF(%s, ''), channel),
                            updated_at = NOW()
                        WHERE id = %s;
                        """,
                        (name or "", email or "", channel or "sms", conv_id),
                    )
                    return {"id": conv_id, "tenant_id": tenant_id, "contact_phone": normalized_phone, "contact_name": name, "contact_email": email, "channel": channel, "status": existing_status}
                conv_id = new_id("conv")
                display_name = name or (phone or "Onbekende klant")
                cur.execute(
                    """
                    INSERT INTO conversations (id, tenant_id, contact_phone, contact_email, contact_name, channel, status, summary)
                    VALUES (%s, %s, %s, %s, %s, %s, 'ai-active', %s);
                    """,
                    (conv_id, tenant_id, normalized_phone, email or "", display_name, channel or "sms", "Nieuw gesprek aangemaakt via Reactify."),
                )
                return {"id": conv_id, "tenant_id": tenant_id, "contact_phone": normalized_phone, "contact_name": display_name, "contact_email": email, "channel": channel, "status": "inactive"}
    except Exception as e:
        log(f"⚠️ get_or_create_conversation failed: {e}")
        return None


def add_conversation_message(conversation_id: str, tenant_id: str, direction: str, body: str, channel: str = "sms", external_id: str = "", sender_type: str = "unknown") -> Optional[str]:
    if not db_available() or not conversation_id or not body:
        return None
    try:
        ensure_conversation_tables()
        msg_id = new_id("msg")
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO conversation_messages (id, conversation_id, tenant_id, direction, channel, body, external_id, sender_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (msg_id, conversation_id, tenant_id, direction, channel or "sms", body, external_id or "", sender_type or "unknown"),
                )
                cur.execute("UPDATE conversations SET updated_at = NOW() WHERE id = %s;", (conversation_id,))
        return msg_id
    except Exception as e:
        log(f"⚠️ add_conversation_message failed: {e}")
        return None


def classify_text_basic(text: str) -> Dict[str, Any]:
    """
    Snelle, betrouwbare status-classificatie voor Reactify Inbox.
    Deze regels draaien vóór of naast Retell, zodat expliciete menselijke-overname woorden
    zoals "medewerker" altijd correct naar Overname nodig gaan.
    """
    t = (text or "").lower()

    wants_booking = any(w in t for w in [
        "afspraak", "boeken", "inplannen", "planning", "wanneer", "beschikbaar",
        "morgen", "vandaag", "deze week", "volgende week", "reservatie", "reserveren"
    ])

    urgent = any(w in t for w in [
        "dringend", "spoed", "vandaag", "asap", "meteen", "snel", "urgent",
        "zo snel mogelijk", "onmiddellijk"
    ])

    negative = any(w in t for w in [
        "boos", "klacht", "klacht indienen", "niet tevreden", "ontevreden",
        "probleem", "fout", "slecht", "teleurgesteld", "annuleren"
    ])

    human_request = any(w in t for w in [
        "medewerker", "een medewerker", "persoon", "mens", "iemand spreken",
        "iemand aan de lijn", "bellen", "bel mij", "bel me", "contact opnemen",
        "zaakvoerder", "manager", "verantwoordelijke", "klantendienst"
    ])

    inactive = any(w in t for w in [
        "ok bedankt", "oke bedankt", "dank u", "bedankt", "merci", "top bedankt",
        "in orde", "is goed", "prima", "tot dan", "afgesproken"
    ]) and not (wants_booking or urgent or negative or human_request)

    requires_human = bool(urgent or negative or human_request)
    intent = "afspraak_maken" if wants_booking else ("menselijke_overname" if human_request else "vraag_stellen")
    urgency = "hoog" if urgent else ("medium" if (negative or human_request) else "normaal")

    if inactive:
        summary = "Het gesprek lijkt afgerond. Er is momenteel geen verdere actie nodig."
        recommended = "Geen actie nodig, tenzij de klant opnieuw reageert."
        suggested = "Graag gedaan. Laat gerust iets weten als ik nog kan helpen."
    elif human_request:
        summary = "De klant vraagt expliciet om een medewerker of menselijke opvolging."
        recommended = "Neem het gesprek over en reageer persoonlijk."
        suggested = "Dag, ik geef dit door aan een medewerker. We komen zo snel mogelijk bij u terug."
    elif negative:
        summary = "De klant lijkt ontevreden of meldt een probleem. Menselijke opvolging is aanbevolen."
        recommended = "Neem het gesprek over, erken het probleem en stel een concrete oplossing voor."
        suggested = "Dag, bedankt om dit te melden. Ik kijk dit meteen na en kom zo snel mogelijk bij u terug."
    elif urgent:
        summary = "De klant vraagt snelle opvolging en verwacht vandaag of zo snel mogelijk reactie."
        recommended = "Neem het gesprek over of geef onmiddellijk een concreet beschikbaar moment."
        suggested = "Dag, ik bekijk dit met prioriteit en kom zo snel mogelijk bij u terug."
    elif wants_booking:
        summary = "De klant wil een afspraak maken of een beschikbaar moment vinden."
        recommended = "Stel twee concrete momenten voor of maak meteen een afspraak via Cal.com."
        suggested = "Dag, ik kan u hiermee helpen. Past woensdag om 10u of donderdag om 14u?"
    else:
        summary = "De klant heeft een vraag of verwacht verdere opvolging."
        recommended = "Laat AI antwoorden, maar volg op als de klant bijkomende vragen stelt."
        suggested = "Dag, bedankt voor uw bericht. Ik kijk dit even na en kom hier zo snel mogelijk op terug."

    return {
        "intent": intent,
        "urgency": urgency,
        "requiresHuman": requires_human,
        "inactive": inactive,
        "summary": summary,
        "recommendedAction": recommended,
        "suggestedReply": suggested,
    }


def update_conversation_ai(conversation_id: str, analysis: Dict[str, Any]) -> None:
    if not db_available() or not conversation_id:
        return
    try:
        status = "afgesloten" if analysis.get("inactive") else ("menselijke_overname" if analysis.get("requiresHuman") else "ai-active")
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE conversations
                    SET intent = %s, urgency = %s, requires_human = %s, summary = %s,
                        recommended_action = %s, suggested_reply = %s, status = %s, updated_at = NOW()
                    WHERE id = %s;
                    """,
                    (analysis.get("intent"), analysis.get("urgency"), bool(analysis.get("requiresHuman")), analysis.get("summary"), analysis.get("recommendedAction"), analysis.get("suggestedReply"), status, conversation_id),
                )
    except Exception as e:
        log(f"⚠️ update_conversation_ai failed: {e}")


# =========================
# SMS SEND (Smstools)
# =========================

def normalize_money_for_sms(text: str) -> str:
    """
    SMS-safe money formatting:
    - Replace euro symbol and EUR with the word 'euro'
    - Convert patterns like '€59' or '59€' to '59 euro'
    - Keep it simple and robust for common cases
    """
    if not text:
        return text

    s = text

    # Normalize EUR tokens
    s = re.sub(r"\bEUR\b", "euro", s, flags=re.IGNORECASE)

    # Convert "€59" / "€ 59" -> "59 euro"
    s = re.sub(r"€\s*([0-9]+(?:[.,][0-9]{1,2})?)", r"\1 euro", s)

    # Convert "59€" / "59 €" -> "59 euro"
    s = re.sub(r"([0-9]+(?:[.,][0-9]{1,2})?)\s*€", r"\1 euro", s)

    # Any remaining '€' becomes 'euro' (fallback)
    s = s.replace("€", "euro")

    # Tidy multiple spaces
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def send_sms(tenant: Dict[str, Any], to_number: str, message: str) -> None:
    if not to_number or not message:
        return
        
    # ✅ Force SMS-safe money formatting for all tenants
    message = normalize_money_for_sms(message)
    
    if not SMSTOOLS_CLIENT_ID or not SMSTOOLS_CLIENT_SECRET:
        log("⚠️ Smstools credentials missing")
        return

    payload = {"message": message, "to": to_number, "sender": tenant["virtual_number"]}
    headers = {
        "X-Client-Id": SMSTOOLS_CLIENT_ID,
        "X-Client-Secret": SMSTOOLS_CLIENT_SECRET,
        "Content-Type": "application/json",
    }

    try:
        r = requests.post(SMSTOOLS_SEND_URL, json=payload, headers=headers, timeout=25)
        log(f"📤 Smstools send status={r.status_code}")
        if 200 <= r.status_code < 300:
            bump_monthly_outbound(tenant["tenant_id"], 1)
        else:
            log(f"⚠️ Smstools send failed: {r.text[:300]}")
    except Exception as e:
        log(f"⚠️ Smstools send error: {e}")


# =========================
# RETELL (simple)
# =========================

def get_or_create_chat_id(tenant: Dict[str, Any], phone: str) -> Optional[str]:
    key = (tenant["tenant_id"], phone)
    if key in SMS_SESSIONS:
        return SMS_SESSIONS[key]

    if not RETELL_API_KEY or not tenant.get("retell_agent_id"):
        return None

    try:
        r = requests.post(
            f"{RETELL_BASE_URL}/create-chat",
            headers={"Authorization": f"Bearer {RETELL_API_KEY}", "Content-Type": "application/json"},
            json={"agent_id": tenant["retell_agent_id"], "metadata": {"phone": phone}},
            timeout=25,
        )
        data = r.json() if r.content else {}
        chat_id = data.get("chat_id") or data.get("id")
        if chat_id:
            SMS_SESSIONS[key] = chat_id
            return chat_id
    except Exception as e:
        log(f"⚠️ Retell create-chat error: {e}")
    return None


def ask_retell_via_sms(tenant: Dict[str, Any], phone: str, text: str) -> str:
    opening = tenant.get("opening_line") or "Bedankt voor je bericht."
    if not RETELL_API_KEY or not tenant.get("retell_agent_id"):
        return opening

    chat_id = get_or_create_chat_id(tenant, phone)
    if not chat_id:
        return opening

    try:
        r = requests.post(
            f"{RETELL_BASE_URL}/create-chat-completion",
            headers={"Authorization": f"Bearer {RETELL_API_KEY}", "Content-Type": "application/json"},
            json={"chat_id": chat_id, "content": text},
            timeout=30,
        )
        data = r.json() if r.content else {}
        for m in reversed(data.get("messages", [])):
            if m.get("role") == "agent":
                content = (m.get("content") or "").strip()
                return content or opening
    except Exception as e:
        log(f"⚠️ Retell completion error: {e}")

    return opening




def is_ai_disabled_status(status: str) -> bool:
    s = (status or "").strip().lower().replace("-", "_").replace(" ", "_")
    return s in ("menselijke_overname", "human_required", "human_needed", "manual_takeover", "manual_overname", "ai_paused", "afgesloten", "inactive", "closed")


def set_conversation_status(conversation_id: str, tenant_id: str, status: str, requires_human: Optional[bool] = None) -> None:
    if not db_available() or not conversation_id:
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                if requires_human is None:
                    cur.execute(
                        """
                        UPDATE conversations
                        SET status = %s, updated_at = NOW()
                        WHERE id = %s AND tenant_id = %s;
                        """,
                        (status, conversation_id, tenant_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE conversations
                        SET status = %s, requires_human = %s, updated_at = NOW()
                        WHERE id = %s AND tenant_id = %s;
                        """,
                        (status, bool(requires_human), conversation_id, tenant_id),
                    )
    except Exception as e:
        log(f"⚠️ set_conversation_status failed: {e}")


# =========================
# ROUTES
# =========================

@app.route("/", methods=["GET"])
def health():
    return "OK", 200


@app.route("/admin/ping", methods=["GET"])
def admin_ping():
    auth = require_admin_token()
    if auth is not None:
        return auth
    return jsonify({"ok": True}), 200


@app.route("/admin/usage", methods=["GET"])
def admin_usage():
    auth = require_admin_token()
    if auth is not None:
        return auth

    if not db_available():
        return jsonify({"data": []}), 200

    try:
        ensure_monthly_usage_table()

        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT month, tenant_id, outbound_count
                    FROM monthly_usage
                    ORDER BY month DESC, tenant_id;
                    """
                )
                rows = cur.fetchall()

        out = []
        for (m, tenant_id, outbound) in rows:
            t = TENANTS_BY_ID.get(tenant_id, {})
            plan = (t.get("plan") or "").strip().lower()
            out.append(
                {
                    "month": m,
                    "company_number": t.get("company_number", ""),
                    "company_name": t.get("company_name", ""),
                    "tenant_id": tenant_id,
                    "stripe_customer_id": t.get("stripe_customer_id", ""),
                    "plan": plan,
                    "outbound": int(outbound or 0),
                    "price_eur": get_overage_price_eur(plan),
                }
            )

        return jsonify({"data": out}), 200

    except Exception as e:
        log(f"❌ /admin/usage error: {e}")
        return jsonify({"error": "internal_error", "data": []}), 500


@app.route("/sms/inbound", methods=["POST"])
def sms_inbound():
    payload = request.get_json(force=True, silent=True)
    event = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(event, dict):
        return "OK", 200

    msg = event.get("message") or {}
    receiver = (msg.get("receiver") or event.get("receiver") or "").strip()
    sender = (msg.get("sender") or event.get("sender") or event.get("from") or "").strip()
    text = (msg.get("content") or event.get("content") or event.get("text") or "").strip()

    tenant = get_tenant_by_receiver(receiver)
    if not tenant:
        log(f"⚠️ /sms/inbound: no tenant for receiver={receiver}")
        return "OK", 200

    if sender and text:
        conv = get_or_create_conversation(tenant, phone=sender, channel="sms")
        analysis = classify_text_basic(text)
        current_status = (conv or {}).get("status") or "ai-active"

        if conv:
            add_conversation_message(conv["id"], tenant["tenant_id"], "incoming", text, "sms", sender_type="customer")
            # Als de ondernemer al heeft overgenomen, blijft AI uit.
            # Als de klant expliciet menselijke hulp nodig heeft, schakelen we AI meteen uit.
            if is_ai_disabled_status(current_status):
                set_conversation_status(conv["id"], tenant["tenant_id"], current_status, True)
                log(f"🤝 AI disabled for conversation={conv['id']} status={current_status}; inbound saved only")
                return "OK", 200
            update_conversation_ai(conv["id"], analysis)
            if analysis.get("requiresHuman"):
                set_conversation_status(conv["id"], tenant["tenant_id"], "menselijke_overname", True)
                log(f"🤝 Human takeover required for conversation={conv['id']}; Retell skipped")
                return "OK", 200

        reply = ask_retell_via_sms(tenant, sender, text)
        send_sms(tenant, sender, reply)

        if conv and reply:
            add_conversation_message(conv["id"], tenant["tenant_id"], "outgoing", reply, "sms", sender_type="ai")
            # Status blijft AI actief, tenzij er ondertussen menselijke overname nodig was.
            if not analysis.get("requiresHuman"):
                set_conversation_status(conv["id"], tenant["tenant_id"], "ai-active", False)

    return "OK", 200


@app.route("/call/missed", methods=["POST"])
def call_missed():
    payload = request.get_json(force=True, silent=True)
    event = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(event, dict):
        return "OK", 200

    msg = event.get("message") or {}
    receiver = (msg.get("receiver") or event.get("receiver") or "").strip()
    caller = (event.get("caller") or event.get("from") or msg.get("sender") or "").strip()

    tenant = get_tenant_by_receiver(receiver)
    if not tenant:
        log(f"⚠️ /call/missed: no tenant for receiver={receiver}")
        return "OK", 200

    if caller:
        opening = tenant.get("opening_line") or "Bedankt om te bellen. Hoe kan ik helpen?"
        conv = get_or_create_conversation(tenant, phone=caller, channel="missed_call")
        if conv:
            add_conversation_message(conv["id"], tenant["tenant_id"], "incoming", "Gemiste oproep", "missed_call", sender_type="system")
            update_conversation_ai(conv["id"], classify_text_basic("Gemiste oproep. Klant verwacht terugkoppeling."))
        send_sms(tenant, caller, opening)
        if conv:
            add_conversation_message(conv["id"], tenant["tenant_id"], "outgoing", opening, "sms", sender_type="ai")

    return "OK", 200





@app.route("/conversations", methods=["GET", "PATCH", "POST", "DELETE"])
def conversations():
    tenant = get_tenant_from_request_or_default()
    if not tenant or not db_available():
        return jsonify({"status": "success", "data": []}), 200
    try:
        ensure_conversation_tables()

        if request.method == "DELETE":
            body = request.get_json(force=True, silent=True) or {}
            conversation_id = (body.get("conversationId") or body.get("conversation_id") or request.args.get("conversationId") or request.args.get("conversation_id") or request.args.get("id") or "").strip()
            phone = normalize_phone(body.get("phone") or body.get("contact_phone") or request.args.get("phone") or request.args.get("contact_phone") or "")
            email = (body.get("email") or body.get("contact_email") or request.args.get("email") or request.args.get("contact_email") or "").strip().lower()
            if not conversation_id and not phone and not email:
                return jsonify({"status": "error", "error": "conversationId, telefoon of e-mail ontbreekt."}), 400
            deleted_ids = []
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    if conversation_id:
                        cur.execute("DELETE FROM conversations WHERE id = %s AND tenant_id = %s RETURNING id;", (conversation_id, tenant["tenant_id"]))
                        deleted_ids.extend([r[0] for r in cur.fetchall()])
                    if phone:
                        cur.execute("DELETE FROM conversations WHERE tenant_id = %s AND contact_phone = %s RETURNING id;", (tenant["tenant_id"], phone))
                        deleted_ids.extend([r[0] for r in cur.fetchall()])
                    if email:
                        cur.execute("DELETE FROM conversations WHERE tenant_id = %s AND LOWER(COALESCE(contact_email,'')) = %s RETURNING id;", (tenant["tenant_id"], email))
                        deleted_ids.extend([r[0] for r in cur.fetchall()])
            return jsonify({"status": "success", "data": {"deleted": True, "ids": list(dict.fromkeys(deleted_ids))}}), 200

        if request.method in ("PATCH", "POST"):
            body = request.get_json(force=True, silent=True) or {}
            conversation_id = (body.get("conversationId") or body.get("conversation_id") or request.args.get("conversationId") or request.args.get("id") or "").strip()
            status = (body.get("status") or body.get("conversationStatus") or "").strip()
            phone = body.get("phone") or body.get("contact_phone") or body.get("to") or ""
            name = body.get("name") or body.get("contact_name") or body.get("customerName") or ""
            email = body.get("email") or body.get("contact_email") or body.get("customerEmail") or ""
            channel = body.get("channel") or "sms"

            # POST zonder status = nieuw gesprek/contact aanmaken vanuit Reactify.
            if request.method == "POST" and not status and (phone or email or name):
                conv = get_or_create_conversation(tenant, phone=phone, name=name, email=email, channel=channel)
                if not conv:
                    return jsonify({"status": "error", "error": "Kon gesprek niet aanmaken."}), 500
                return jsonify({"status": "success", "data": conv}), 200

            ai_enabled = body.get("aiEnabled")
            if ai_enabled is not None and not status:
                status = "ai-active" if bool(ai_enabled) else "manual_overname"
            if not conversation_id:
                return jsonify({"status": "error", "error": "conversationId ontbreekt."}), 400

            normalized_phone = normalize_phone(phone) if phone else ""
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE conversations
                        SET contact_name = COALESCE(NULLIF(%s, ''), contact_name),
                            contact_email = COALESCE(NULLIF(%s, ''), contact_email),
                            contact_phone = COALESCE(NULLIF(%s, ''), contact_phone),
                            updated_at = NOW()
                        WHERE id = %s AND tenant_id = %s;
                        """,
                        (name.strip(), email.strip().lower(), normalized_phone, conversation_id, tenant["tenant_id"]),
                    )

            if status:
                requires_human = status.strip().lower().replace("-", "_") not in ("ai_active", "ai_actief")
                set_conversation_status(conversation_id, tenant["tenant_id"], status, requires_human)
            else:
                requires_human = None

            return jsonify({"status": "success", "data": {"id": conversation_id, "status": status or None, "aiEnabled": None if requires_human is None else not requires_human}}), 200

        limit = max(1, min(200, to_int_safe(request.args.get("limit"), 100)))
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.id, c.tenant_id, c.contact_phone, c.contact_email, c.contact_name,
                           c.channel, c.status, c.intent, c.urgency, c.requires_human, c.summary,
                           c.recommended_action, c.suggested_reply, c.created_at, c.updated_at,
                           lm.body AS last_message, lm.created_at AS last_message_at, lm.direction AS last_message_direction
                    FROM conversations c
                    LEFT JOIN LATERAL (
                      SELECT body, created_at, direction FROM conversation_messages m
                      WHERE m.conversation_id = c.id ORDER BY created_at DESC LIMIT 1
                    ) lm ON TRUE
                    WHERE c.tenant_id = %s
                    ORDER BY c.updated_at DESC
                    LIMIT %s;
                    """,
                    (tenant["tenant_id"], limit),
                )
                rows = cur.fetchall()
        data = []
        for r in rows:
            data.append({
                "id": r[0], "tenant_id": r[1], "contact_phone": r[2] or "", "contact_email": r[3] or "",
                "contact_name": r[4] or r[2] or "Onbekende klant", "channel": r[5] or "sms", "status": r[6] or "ai-active",
                "intent": r[7] or "", "urgency": r[8] or "", "requires_human": bool(r[9]), "summary": r[10] or "",
                "recommended_action": r[11] or "", "suggested_reply": r[12] or "",
                "created_at": r[13].isoformat() if r[13] else None, "updated_at": r[14].isoformat() if r[14] else None,
                "last_message": r[15] or "", "last_message_at": r[16].isoformat() if r[16] else None, "last_message_direction": r[17] or "",
            })
        return jsonify({"status": "success", "data": data}), 200
    except Exception as e:
        log(f"❌ /conversations error: {e}")
        return jsonify({"status": "error", "error": "Kon gesprekken niet ophalen.", "details": str(e)}), 500


@app.route("/conversation-messages", methods=["GET"])
def conversation_messages():
    tenant = get_tenant_from_request_or_default()
    conversation_id = (request.args.get("conversationId") or request.args.get("conversation_id") or request.args.get("id") or "").strip()
    if not conversation_id:
        return jsonify({"status": "error", "error": "conversationId ontbreekt."}), 400
    if not tenant or not db_available():
        return jsonify({"status": "success", "data": []}), 200
    try:
        ensure_conversation_tables()
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT m.id, m.conversation_id, m.tenant_id, m.direction, m.channel, m.body, m.external_id, m.created_at, m.sender_type
                    FROM conversation_messages m
                    INNER JOIN conversations c ON c.id = m.conversation_id AND c.tenant_id = m.tenant_id
                    WHERE m.conversation_id = %s AND c.tenant_id = %s
                    ORDER BY m.created_at ASC;
                    """,
                    (conversation_id, tenant["tenant_id"]),
                )
                rows = cur.fetchall()
        data = [{
            "id": r[0], "conversation_id": r[1], "tenant_id": r[2], "direction": r[3], "channel": r[4],
            "body": r[5], "text": r[5], "external_id": r[6] or "", "created_at": r[7].isoformat() if r[7] else None, "sender_type": r[8] or ("customer" if r[3] == "incoming" else "unknown"),
        } for r in rows]
        return jsonify({"status": "success", "data": data}), 200
    except Exception as e:
        log(f"❌ /conversation-messages error: {e}")
        return jsonify({"status": "error", "error": "Kon berichten niet ophalen.", "details": str(e)}), 500


@app.route("/send-sms", methods=["POST"])

def inbox_send_sms():
    body = request.get_json(force=True, silent=True) or {}
    conversation_id = (body.get("conversationId") or body.get("conversation_id") or "").strip()
    message = (body.get("message") or body.get("text") or "").strip()
    if not conversation_id:
        return jsonify({"status": "error", "error": "conversationId ontbreekt."}), 400
    if not message:
        return jsonify({"status": "error", "error": "Bericht ontbreekt."}), 400
    if not db_available():
        return jsonify({"status": "error", "error": "DATABASE_URL ontbreekt."}), 500
    try:
        ensure_conversation_tables()
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT tenant_id, contact_phone FROM conversations WHERE id = %s LIMIT 1;", (conversation_id,))
                row = cur.fetchone()
        if not row:
            return jsonify({"status": "error", "error": "Gesprek niet gevonden."}), 404
        tenant_id, phone = row
        tenant = TENANTS_BY_ID.get(tenant_id)
        if not tenant:
            return jsonify({"status": "error", "error": "Tenant niet gevonden."}), 404
        if not phone:
            return jsonify({"status": "error", "error": "Geen telefoonnummer gekoppeld aan dit gesprek."}), 400
        send_sms(tenant, phone, message)
        msg_id = add_conversation_message(conversation_id, tenant_id, "outgoing", message, "sms", sender_type="manual")
        # Elk manueel bericht vanuit Reactify betekent: ondernemer heeft overgenomen, AI blijft uit.
        set_conversation_status(conversation_id, tenant_id, "manual_overname", True)
        return jsonify({"status": "success", "data": {"id": msg_id, "conversationId": conversation_id, "status": "manual_overname", "aiEnabled": False}}), 200
    except Exception as e:
        log(f"❌ /send-sms error: {e}")
        return jsonify({"status": "error", "error": "SMS verzenden mislukt.", "details": str(e)}), 500


@app.route("/classify-conversation", methods=["POST"])
def classify_conversation():
    body = request.get_json(force=True, silent=True) or {}
    conversation_id = (body.get("conversationId") or body.get("conversation_id") or "").strip()
    if not conversation_id:
        return jsonify({"status": "error", "error": "conversationId ontbreekt."}), 400
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT body FROM conversation_messages WHERE conversation_id = %s ORDER BY created_at DESC LIMIT 10;", (conversation_id,))
                text = "\n".join(r[0] for r in cur.fetchall())
        analysis = classify_text_basic(text)
        update_conversation_ai(conversation_id, analysis)
        return jsonify({"status": "success", "data": analysis}), 200
    except Exception as e:
        log(f"❌ /classify-conversation error: {e}")
        return jsonify({"status": "error", "error": "Classificatie mislukt.", "details": str(e)}), 500


# =========================
# STARTUP
# =========================

load_tenants_from_csv(TENANTS_CSV_PATH)
ensure_monthly_usage_table()
ensure_conversation_tables()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

