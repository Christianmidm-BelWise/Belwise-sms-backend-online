import os
import re
import csv
import uuid
import json
import ssl
import imaplib
import smtplib
import threading
import base64
import hashlib
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import parseaddr, formataddr, make_msgid
from html import unescape
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple

import requests
import psycopg2
from psycopg2 import errors as pg_errors
from flask import Flask, request, jsonify
from cryptography.fernet import Fernet, InvalidToken

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
EMAIL_ENCRYPTION_KEY = (os.environ.get("EMAIL_ENCRYPTION_KEY") or "").strip()
EMAIL_SYNC_LOCK = threading.Lock()


def log(msg: str) -> None:
    if DEBUG_LOGS:
        print(msg, flush=True)


@app.before_request
def _log_request() -> None:
    try:
        log(f"➡️ {request.method} {request.path} qs={request.query_string.decode('utf-8', 'ignore')}")
    except Exception:
        pass


@app.before_request
def _privacy_cleanup_tick() -> None:
    if request.path not in ("/health",):
        run_retention_cleanup(force=False)


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
# PRIVACY / DATA RETENTION
# =========================

_RETENTION_LAST_RUN: Optional[datetime] = None


def ensure_privacy_settings_table() -> None:
    if not db_available():
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tenant_privacy_settings (
                      tenant_id TEXT PRIMARY KEY,
                      retention_days INT NOT NULL DEFAULT 90 CHECK (retention_days IN (60, 90)),
                      profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                """)
    except Exception as exc:
        log(f"⚠️ ensure_privacy_settings_table failed: {exc}")


def run_retention_cleanup(force: bool = False) -> int:
    global _RETENTION_LAST_RUN
    if not db_available():
        return 0
    now = datetime.now(timezone.utc)
    if not force and _RETENTION_LAST_RUN and now - _RETENTION_LAST_RUN < timedelta(minutes=15):
        return 0
    _RETENTION_LAST_RUN = now
    ensure_privacy_settings_table()
    ensure_conversation_tables()
    deleted = 0
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM conversations c
                    USING tenant_privacy_settings s
                    WHERE c.tenant_id = s.tenant_id
                      AND c.updated_at < NOW() - (s.retention_days * INTERVAL '1 day')
                    RETURNING c.id;
                """)
                deleted = len(cur.fetchall())
        if deleted:
            log(f"🧹 Privacy cleanup removed {deleted} conversations")
    except Exception as exc:
        log(f"⚠️ retention cleanup failed: {exc}")
    return deleted


@app.route("/privacy-settings", methods=["GET", "PATCH", "POST", "DELETE"])
def privacy_settings():
    tenant = get_tenant_from_request_or_default()
    if not tenant:
        return jsonify({"status": "error", "error": "Geen platformtenant geselecteerd."}), 400
    if not db_available():
        return jsonify({"status": "error", "error": "DATABASE_URL ontbreekt."}), 500
    ensure_privacy_settings_table()
    ensure_conversation_tables()
    tenant_id = tenant["tenant_id"]
    try:
        if request.method == "DELETE":
            body = request.get_json(force=True, silent=True) or {}
            if body.get("confirm") != "VERWIJDEREN":
                return jsonify({"status": "error", "error": "Bevestiging ontbreekt."}), 400
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM conversations WHERE tenant_id = %s RETURNING id;", (tenant_id,))
                    deleted = len(cur.fetchall())
            return jsonify({"status": "success", "data": {"deletedConversations": deleted}}), 200

        if request.method in ("PATCH", "POST"):
            body = request.get_json(force=True, silent=True) or {}
            retention = to_int_safe(body.get("retentionDays") or body.get("retention_days"), 90)
            if retention not in (60, 90):
                return jsonify({"status": "error", "error": "Bewaartermijn moet 60 of 90 dagen zijn."}), 400
            profile = body.get("profile") if isinstance(body.get("profile"), dict) else None
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    if profile is None:
                        cur.execute("""
                            INSERT INTO tenant_privacy_settings (tenant_id, retention_days)
                            VALUES (%s, %s)
                            ON CONFLICT (tenant_id) DO UPDATE SET retention_days = EXCLUDED.retention_days, updated_at = NOW();
                        """, (tenant_id, retention))
                    else:
                        cur.execute("""
                            INSERT INTO tenant_privacy_settings (tenant_id, retention_days, profile_json)
                            VALUES (%s, %s, %s::jsonb)
                            ON CONFLICT (tenant_id) DO UPDATE SET retention_days = EXCLUDED.retention_days,
                              profile_json = tenant_privacy_settings.profile_json || EXCLUDED.profile_json, updated_at = NOW();
                        """, (tenant_id, retention, json.dumps(profile)))
            run_retention_cleanup(force=True)

        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO tenant_privacy_settings (tenant_id) VALUES (%s)
                    ON CONFLICT (tenant_id) DO NOTHING;
                """, (tenant_id,))
                cur.execute("SELECT retention_days, profile_json, updated_at FROM tenant_privacy_settings WHERE tenant_id = %s;", (tenant_id,))
                row = cur.fetchone()
        return jsonify({"status": "success", "data": {"retentionDays": row[0], "profile": row[1] or {}, "updatedAt": row[2].isoformat() if row[2] else None}}), 200
    except Exception as exc:
        log(f"❌ /privacy-settings error: {exc}")
        return jsonify({"status": "error", "error": "Privacy-instellingen konden niet worden verwerkt.", "details": str(exc)}), 500

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
                      subject TEXT,
                      external_thread_id TEXT,
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
                      subject TEXT,
                      html_body TEXT,
                      external_id TEXT,
                      external_thread_id TEXT,
                      in_reply_to TEXT,
                      sender_type TEXT NOT NULL DEFAULT 'unknown',
                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );

                    CREATE INDEX IF NOT EXISTS idx_conversations_tenant_updated
                      ON conversations (tenant_id, updated_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_conversations_phone
                      ON conversations (tenant_id, contact_phone);
                    ALTER TABLE conversations ADD COLUMN IF NOT EXISTS subject TEXT;
                    ALTER TABLE conversations ADD COLUMN IF NOT EXISTS external_thread_id TEXT;
                    ALTER TABLE conversation_messages ADD COLUMN IF NOT EXISTS subject TEXT;
                    ALTER TABLE conversation_messages ADD COLUMN IF NOT EXISTS html_body TEXT;
                    ALTER TABLE conversation_messages ADD COLUMN IF NOT EXISTS external_thread_id TEXT;
                    ALTER TABLE conversation_messages ADD COLUMN IF NOT EXISTS in_reply_to TEXT;
                    ALTER TABLE conversation_messages ADD COLUMN IF NOT EXISTS sender_type TEXT NOT NULL DEFAULT 'unknown';
                    UPDATE conversation_messages SET sender_type = CASE WHEN direction = 'incoming' THEN 'customer' ELSE 'unknown' END WHERE sender_type IS NULL OR sender_type = '';
                    UPDATE conversations SET contact_phone = CASE WHEN contact_phone ~ '^32' THEN '+' || contact_phone WHEN contact_phone ~ '^0' THEN '+32' || SUBSTRING(contact_phone FROM 2) ELSE contact_phone END WHERE contact_phone IS NOT NULL AND contact_phone <> '';

                    CREATE INDEX IF NOT EXISTS idx_messages_conversation_created
                      ON conversation_messages (conversation_id, created_at ASC);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_external_unique
                      ON conversation_messages (tenant_id, external_id)
                      WHERE external_id IS NOT NULL AND external_id <> '';

                    CREATE TABLE IF NOT EXISTS tenant_email_settings (
                      tenant_id TEXT PRIMARY KEY,
                      enabled BOOLEAN NOT NULL DEFAULT FALSE,
                      email_address TEXT NOT NULL DEFAULT '',
                      sender_name TEXT NOT NULL DEFAULT '',
                      imap_host TEXT NOT NULL DEFAULT '',
                      imap_port INT NOT NULL DEFAULT 993,
                      imap_security TEXT NOT NULL DEFAULT 'ssl',
                      smtp_host TEXT NOT NULL DEFAULT '',
                      smtp_port INT NOT NULL DEFAULT 587,
                      smtp_security TEXT NOT NULL DEFAULT 'starttls',
                      username TEXT NOT NULL DEFAULT '',
                      password_encrypted TEXT NOT NULL DEFAULT '',
                      signature TEXT NOT NULL DEFAULT '',
                      auto_reply BOOLEAN NOT NULL DEFAULT TRUE,
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
        log("✅ conversation tables ensured")
    except Exception as e:
        log(f"⚠️ ensure_conversation_tables failed: {e}")


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def get_default_tenant() -> Optional[Dict[str, Any]]:
    # A tenant is the business account using Reactify, not an end-customer/contact.
    # The current logged-in platform account must map to exactly one tenant.
    if PREFERRED_TENANT_ID and PREFERRED_TENANT_ID in TENANTS_BY_ID:
        return TENANTS_BY_ID[PREFERRED_TENANT_ID]
    if len(TENANTS_BY_ID) == 1:
        return next(iter(TENANTS_BY_ID.values()))
    log("⚠️ No platform tenant selected. Set REACTIFY_TENANT_ID to the tenant_id of the logged-in business account.")
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


def get_conversation_tenant(conversation_id: str) -> Optional[Dict[str, Any]]:
    """Resolve the tenant from the stored conversation instead of trusting the dashboard header."""
    if not db_available() or not conversation_id:
        return None
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT tenant_id FROM conversations WHERE id = %s LIMIT 1;", (conversation_id,))
                row = cur.fetchone()
        return TENANTS_BY_ID.get(row[0]) if row else None
    except Exception as exc:
        log(f"⚠️ get_conversation_tenant failed: {exc}")
        return None


def get_or_create_conversation(tenant: Dict[str, Any], phone: str = "", name: str = "", email: str = "", channel: str = "sms", subject: str = "", external_thread_id: str = "") -> Optional[Dict[str, Any]]:
    if not db_available() or not tenant:
        return None
    normalized_phone = normalize_phone(phone) if phone else ""
    normalized_email = (email or "").strip().lower()
    normalized_channel = (channel or "sms").strip().lower()
    tenant_id = tenant["tenant_id"]
    try:
        ensure_conversation_tables()
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                existing = None
                if normalized_channel == "email" and external_thread_id:
                    cur.execute("""
                        SELECT id, status, updated_at FROM conversations
                        WHERE tenant_id = %s AND channel = 'email' AND external_thread_id = %s
                        ORDER BY updated_at DESC LIMIT 1;
                    """, (tenant_id, external_thread_id))
                    existing = cur.fetchone()
                if not existing and normalized_channel == "email" and normalized_email:
                    cur.execute("""
                        SELECT id, status, updated_at FROM conversations
                        WHERE tenant_id = %s AND channel = 'email' AND LOWER(COALESCE(contact_email,'')) = %s
                          AND LOWER(COALESCE(subject,'')) = LOWER(%s)
                        ORDER BY updated_at DESC LIMIT 1;
                    """, (tenant_id, normalized_email, subject or ""))
                    existing = cur.fetchone()
                if not existing and normalized_channel == "email" and normalized_email:
                    cur.execute("""
                        SELECT id, status, updated_at FROM conversations
                        WHERE tenant_id = %s AND channel = 'email' AND LOWER(COALESCE(contact_email,'')) = %s
                        ORDER BY updated_at DESC LIMIT 1;
                    """, (tenant_id, normalized_email))
                    existing = cur.fetchone()
                if not existing and normalized_channel == "sms" and normalized_phone:
                    cur.execute("""
                        SELECT id, status, updated_at FROM conversations
                        WHERE tenant_id = %s AND channel = 'sms' AND contact_phone = %s
                        ORDER BY updated_at DESC LIMIT 1;
                    """, (tenant_id, normalized_phone))
                    existing = cur.fetchone()
                if existing:
                    conv_id = existing[0]
                    existing_status = existing[1] if len(existing) > 1 else 'ai-active'
                    existing_updated_at = existing[2] if len(existing) > 2 else None
                    if existing_updated_at and datetime.now(timezone.utc) - existing_updated_at > timedelta(minutes=30):
                        existing_status = 'inactive'
                        cur.execute("UPDATE conversations SET status = 'inactive', requires_human = FALSE WHERE id = %s;", (conv_id,))
                    cur.execute("""
                        UPDATE conversations
                        SET contact_name = COALESCE(NULLIF(%s, ''), contact_name),
                            contact_email = COALESCE(NULLIF(%s, ''), contact_email),
                            contact_phone = COALESCE(NULLIF(%s, ''), contact_phone),
                            channel = %s,
                            subject = COALESCE(NULLIF(%s, ''), subject),
                            external_thread_id = COALESCE(NULLIF(%s, ''), external_thread_id),
                            updated_at = NOW()
                        WHERE id = %s;
                    """, (name or "", normalized_email, normalized_phone, normalized_channel, subject or "", external_thread_id or "", conv_id))
                    return {"id": conv_id, "tenant_id": tenant_id, "contact_phone": normalized_phone, "contact_name": name, "contact_email": normalized_email, "channel": normalized_channel, "subject": subject, "status": existing_status}
                conv_id = new_id("conv")
                display_name = name or (normalized_email if normalized_channel == "email" else phone) or "Onbekende klant"
                cur.execute("""
                    INSERT INTO conversations (id, tenant_id, contact_phone, contact_email, contact_name, channel, status, summary, subject, external_thread_id)
                    VALUES (%s, %s, %s, %s, %s, %s, 'ai-active', %s, %s, %s);
                """, (conv_id, tenant_id, normalized_phone, normalized_email, display_name, normalized_channel, "", subject or "", external_thread_id or ""))
                return {"id": conv_id, "tenant_id": tenant_id, "contact_phone": normalized_phone, "contact_name": display_name, "contact_email": normalized_email, "channel": normalized_channel, "subject": subject, "status": "inactive"}
    except Exception as e:
        log(f"⚠️ get_or_create_conversation failed: {e}")
        return None


def add_conversation_message(conversation_id: str, tenant_id: str, direction: str, body: str, channel: str = "sms", external_id: str = "", sender_type: str = "unknown", subject: str = "", html_body: str = "", external_thread_id: str = "", in_reply_to: str = "") -> Optional[str]:
    if not db_available() or not conversation_id or not body:
        return None
    try:
        ensure_conversation_tables()
        msg_id = new_id("msg")
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO conversation_messages (id, conversation_id, tenant_id, direction, channel, body, subject, html_body, external_id, external_thread_id, in_reply_to, sender_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id;
                """, (msg_id, conversation_id, tenant_id, direction, channel or "sms", body, subject or "", html_body or "", external_id or "", external_thread_id or "", in_reply_to or "", sender_type or "unknown"))
                inserted = cur.fetchone()
                if not inserted:
                    return None
                cur.execute("""
                    UPDATE conversations
                    SET updated_at = NOW(), subject = COALESCE(NULLIF(%s, ''), subject),
                        external_thread_id = COALESCE(NULLIF(%s, ''), external_thread_id)
                    WHERE id = %s;
                """, (subject or "", external_thread_id or "", conversation_id))
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
    t = (text or "").lower().strip()
    compact = re.sub(r"[\s.!?,;:]+", " ", t).strip()
    greeting_only = compact in {"hoi", "hallo", "hey", "heey", "hi", "dag", "goedendag", "goeiedag", "goedemorgen", "goedenavond", "yo", "hoi daar", "hallo daar"}

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
    urgency = "hoog" if (urgent or negative or human_request) else "normaal"

    if greeting_only:
        intent = "begroeting"
        urgency = "normaal"
        requires_human = False
        inactive = False
        summary = "De klant begroet de assistent via SMS."
        recommended = "Laat de AI vriendelijk begroeten en vragen waarmee de klant geholpen kan worden."
        suggested = "Hallo! Waarmee kan ik u helpen?"
    elif inactive:
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

def send_sms(tenant: Dict[str, Any], to_number: str, message: str) -> bool:
    if not to_number or not message:
        return False
        
    # ✅ Force SMS-safe money formatting for all tenants
    message = normalize_money_for_sms(message)
    
    if not SMSTOOLS_CLIENT_ID or not SMSTOOLS_CLIENT_SECRET:
        log("⚠️ Smstools credentials missing")
        return False

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
            return True
        log(f"⚠️ Smstools send failed: {r.text[:300]}")
        return False
    except Exception as e:
        log(f"⚠️ Smstools send error: {e}")
        return False


# =========================
# E-MAIL (universele IMAP/SMTP-connector)
# =========================

def _email_cipher() -> Optional[Fernet]:
    """Return a stable Fernet cipher for stored mailbox credentials.

    Preferred: EMAIL_ENCRYPTION_KEY from Render. For backwards-compatible
    deployments where that variable was not created yet, derive a stable key
    from an existing server-side secret. This avoids exposing credentials in
    the browser and prevents the settings form from failing on first setup.
    """
    raw = EMAIL_ENCRYPTION_KEY
    if raw:
        try:
            return Fernet(raw.encode("utf-8"))
        except Exception:
            log("⚠️ EMAIL_ENCRYPTION_KEY is ongeldig; veilige fallback wordt gebruikt.")

    fallback_secret = ADMIN_TOKEN or SMSTOOLS_CLIENT_SECRET or RETELL_API_KEY
    if not fallback_secret:
        log("⚠️ Geen server-side secret beschikbaar om e-mailwachtwoorden te versleutelen.")
        return None

    digest = hashlib.sha256(("reactify-email:" + fallback_secret).encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def encrypt_email_secret(value: str) -> str:
    if not value:
        return ""
    cipher = _email_cipher()
    if not cipher:
        raise ValueError("Geen geldige server-side encryptiesleutel beschikbaar.")
    return cipher.encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_email_secret(value: str) -> str:
    if not value:
        return ""
    cipher = _email_cipher()
    if not cipher:
        raise ValueError("Geen geldige server-side encryptiesleutel beschikbaar.")
    try:
        return cipher.decrypt(value.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise ValueError("Het opgeslagen e-mailwachtwoord kan niet worden ontsleuteld.") from exc


def get_email_settings(tenant_id: str, include_password: bool = False) -> Optional[Dict[str, Any]]:
    if not db_available() or not tenant_id:
        return None
    ensure_conversation_tables()
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT enabled, email_address, sender_name, imap_host, imap_port, imap_security,
                       smtp_host, smtp_port, smtp_security, username, password_encrypted,
                       signature, auto_reply, updated_at
                FROM tenant_email_settings WHERE tenant_id = %s;
            """, (tenant_id,))
            row = cur.fetchone()
    if not row:
        return None
    result = {
        "enabled": bool(row[0]), "emailAddress": row[1] or "", "senderName": row[2] or "",
        "imapHost": row[3] or "", "imapPort": int(row[4] or 993), "imapSecurity": row[5] or "ssl",
        "smtpHost": row[6] or "", "smtpPort": int(row[7] or 587), "smtpSecurity": row[8] or "starttls",
        "username": row[9] or "", "hasPassword": bool(row[10]), "signature": row[11] or "",
        "autoReply": bool(row[12]), "updatedAt": row[13].isoformat() if row[13] else None,
    }
    if include_password:
        result["password"] = decrypt_email_secret(row[10] or "")
    return result


def _strip_html(html: str) -> str:
    if not html:
        return ""
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return re.sub(r"[ \t]+", " ", unescape(text)).strip()


def _email_bodies(message) -> Tuple[str, str]:
    text_body, html_body = "", ""
    if message.is_multipart():
        for part in message.walk():
            if part.get_content_disposition() == "attachment":
                continue
            content_type = part.get_content_type()
            try:
                content = part.get_content()
            except Exception:
                continue
            if content_type == "text/plain" and not text_body:
                text_body = str(content or "").strip()
            elif content_type == "text/html" and not html_body:
                html_body = str(content or "").strip()
    else:
        try:
            content = str(message.get_content() or "").strip()
        except Exception:
            content = ""
        if message.get_content_type() == "text/html":
            html_body = content
        else:
            text_body = content
    if not text_body and html_body:
        text_body = _strip_html(html_body)
    return text_body.strip(), html_body.strip()


def _normalized_subject(value: str) -> str:
    subject = re.sub(r"^(?:(?:re|fw|fwd)\s*:\s*)+", "", (value or "").strip(), flags=re.I)
    return subject[:500]


def _imap_connect(settings: Dict[str, Any]):
    host, port = settings["imapHost"], int(settings.get("imapPort") or 993)
    security = (settings.get("imapSecurity") or "ssl").lower()
    if security == "ssl":
        client = imaplib.IMAP4_SSL(host, port, ssl_context=ssl.create_default_context())
    else:
        client = imaplib.IMAP4(host, port)
        if security == "starttls":
            client.starttls(ssl_context=ssl.create_default_context())
    client.login(settings.get("username") or settings.get("emailAddress"), settings.get("password") or "")
    return client


def send_email_message(tenant: Dict[str, Any], to_email: str, subject: str, body: str, in_reply_to: str = "", references: str = "") -> Tuple[bool, str, str]:
    settings = get_email_settings(tenant["tenant_id"], include_password=True)
    if not settings or not settings.get("enabled"):
        return False, "", "E-mailkanaal is niet geconfigureerd of staat uit."
    if not to_email or not body:
        return False, "", "Ontvanger of bericht ontbreekt."
    msg = EmailMessage()
    sender_address = settings.get("emailAddress") or settings.get("username")
    msg["From"] = formataddr((settings.get("senderName") or tenant.get("company_name") or "", sender_address))
    msg["To"] = to_email
    msg["Subject"] = subject or "Bericht van " + (tenant.get("company_name") or "Reactify")
    external_id = make_msgid(domain=sender_address.split("@")[-1] if "@" in sender_address else None)
    msg["Message-ID"] = external_id
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
    if references:
        msg["References"] = references
    signature = (settings.get("signature") or "").strip()
    full_body = body.strip() + (("\n\n" + signature) if signature else "")
    msg.set_content(full_body)
    try:
        host, port = settings["smtpHost"], int(settings.get("smtpPort") or 587)
        security = (settings.get("smtpSecurity") or "starttls").lower()
        if security == "ssl":
            smtp = smtplib.SMTP_SSL(host, port, context=ssl.create_default_context(), timeout=30)
        else:
            smtp = smtplib.SMTP(host, port, timeout=30)
            smtp.ehlo()
            if security == "starttls":
                smtp.starttls(context=ssl.create_default_context())
                smtp.ehlo()
        smtp.login(settings.get("username") or sender_address, settings.get("password") or "")
        smtp.send_message(msg)
        smtp.quit()
        return True, external_id, ""
    except Exception as exc:
        log(f"⚠️ SMTP send failed: {exc}")
        return False, "", str(exc)


def _last_email_thread_headers(conversation_id: str) -> Tuple[str, str]:
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT external_id, in_reply_to FROM conversation_messages
                WHERE conversation_id = %s AND channel = 'email' AND external_id <> ''
                ORDER BY created_at DESC LIMIT 1;
            """, (conversation_id,))
            row = cur.fetchone()
    if not row:
        return "", ""
    message_id = row[0] or ""
    return message_id, message_id


def sync_incoming_email(tenant: Dict[str, Any], limit: int = 25) -> Dict[str, Any]:
    settings = get_email_settings(tenant["tenant_id"], include_password=True)
    if not settings or not settings.get("enabled"):
        return {"processed": 0, "replied": 0, "enabled": False}
    if not EMAIL_SYNC_LOCK.acquire(blocking=False):
        return {"processed": 0, "replied": 0, "busy": True, "enabled": True}
    processed = replied = 0
    client = None
    try:
        client = _imap_connect(settings)
        client.select("INBOX")
        status, data = client.search(None, "UNSEEN")
        if status != "OK":
            return {"processed": 0, "replied": 0, "enabled": True}
        ids = (data[0] or b"").split()[-max(1, min(limit, 100)):]
        for item_id in ids:
            status, rows = client.fetch(item_id, "(RFC822)")
            if status != "OK" or not rows:
                continue
            raw = next((row[1] for row in rows if isinstance(row, tuple) and len(row) > 1), None)
            if not raw:
                continue
            message = BytesParser(policy=policy.default).parsebytes(raw)
            from_name, from_email = parseaddr(str(message.get("From") or ""))
            from_email = from_email.strip().lower()
            own_email = (settings.get("emailAddress") or "").strip().lower()
            if not from_email or from_email == own_email:
                client.store(item_id, "+FLAGS", "\\Seen")
                continue
            external_id = str(message.get("Message-ID") or "").strip() or f"imap:{item_id.decode(errors='ignore')}"
            subject = str(message.get("Subject") or "").strip() or "Zonder onderwerp"
            normalized_subject = _normalized_subject(subject)
            in_reply_to = str(message.get("In-Reply-To") or "").strip()
            references = str(message.get("References") or "").strip()
            external_thread_id = in_reply_to or (references.split()[0] if references else "") or normalized_subject
            text_body, html_body = _email_bodies(message)
            if not text_body:
                client.store(item_id, "+FLAGS", "\\Seen")
                continue
            conv = get_or_create_conversation(tenant, name=from_name or from_email, email=from_email, channel="email", subject=normalized_subject, external_thread_id=external_thread_id)
            if not conv:
                continue
            inserted = add_conversation_message(conv["id"], tenant["tenant_id"], "incoming", text_body, "email", external_id=external_id, sender_type="customer", subject=subject, html_body=html_body, external_thread_id=external_thread_id, in_reply_to=in_reply_to)
            client.store(item_id, "+FLAGS", "\\Seen")
            if not inserted:
                continue
            processed += 1
            analysis = classify_text_basic(subject + "\n" + text_body)
            current_status = conv.get("status") or "ai-active"
            update_conversation_ai(conv["id"], analysis)
            if is_ai_disabled_status(current_status) or analysis.get("requiresHuman") or not settings.get("autoReply"):
                if analysis.get("requiresHuman"):
                    set_conversation_status(conv["id"], tenant["tenant_id"], "menselijke_overname", True)
                continue
            reply = ask_retell_via_email(tenant, from_email, subject, text_body)
            if not reply:
                continue
            reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"
            ok, sent_id, error = send_email_message(tenant, from_email, reply_subject, reply, in_reply_to=external_id, references=(references + " " + external_id).strip())
            if ok:
                add_conversation_message(conv["id"], tenant["tenant_id"], "outgoing", reply, "email", external_id=sent_id, sender_type="ai", subject=reply_subject, external_thread_id=external_thread_id, in_reply_to=external_id)
                set_conversation_status(conv["id"], tenant["tenant_id"], "ai-active", False)
                replied += 1
            else:
                log(f"⚠️ Automatic email reply failed: {error}")
        return {"processed": processed, "replied": replied, "enabled": True}
    finally:
        try:
            if client:
                client.logout()
        except Exception:
            pass
        EMAIL_SYNC_LOCK.release()

# =========================
# RETELL (simple)
# =========================


def get_contact_context(tenant_id: str, phone: str) -> Dict[str, str]:
    context = {"customer_name": "", "customer_email": "", "has_known_contact_data": "false"}
    if not db_available() or not tenant_id or not phone:
        return context
    try:
        normalized = normalize_phone(phone)
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(contact_name, ''), COALESCE(contact_email, '')
                    FROM conversations
                    WHERE tenant_id = %s AND contact_phone = %s
                    ORDER BY updated_at DESC
                    LIMIT 1;
                    """,
                    (tenant_id, normalized),
                )
                row = cur.fetchone()
        if row:
            name = (row[0] or "").strip()
            email = (row[1] or "").strip().lower()
            if name and normalize_phone(name) != normalized:
                context["customer_name"] = name
            if email:
                context["customer_email"] = email
            context["has_known_contact_data"] = "true" if context["customer_name"] and context["customer_email"] else "false"
    except Exception as exc:
        log(f"⚠️ get_contact_context failed: {exc}")
    return context

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
            json={
                "agent_id": tenant["retell_agent_id"],
                "metadata": {"contact": phone},
                "retell_llm_dynamic_variables": {
                    **get_contact_context(tenant["tenant_id"], phone),
                    "customer_phone": normalize_phone(phone) if not str(phone).startswith("email:") else "",
                    "customer_email": str(phone).split(":", 1)[1] if str(phone).startswith("email:") else get_contact_context(tenant["tenant_id"], phone).get("customer_email", ""),
                    "channel": "email" if str(phone).startswith("email:") else "sms",
                },
            },
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






def ask_retell_via_email(tenant: Dict[str, Any], email_address: str, subject: str, text: str) -> str:
    opening = tenant.get("opening_line") or "Bedankt voor uw e-mail."
    if not RETELL_API_KEY or not tenant.get("retell_agent_id"):
        return opening
    session_key = f"email:{email_address.strip().lower()}"
    chat_id = get_or_create_chat_id(tenant, session_key)
    if not chat_id:
        return opening
    prompt = (
        "Je antwoordt nu via e-mail. Schrijf een professionele, natuurlijke e-mail in het Nederlands. "
        "Gebruik geen SMS-afkortingen. Voeg geen onderwerpregel toe in de tekst en herhaal de volledige e-mail niet.\n\n"
        f"Onderwerp: {subject}\nBericht van klant:\n{text}"
    )
    try:
        r = requests.post(
            f"{RETELL_BASE_URL}/create-chat-completion",
            headers={"Authorization": f"Bearer {RETELL_API_KEY}", "Content-Type": "application/json"},
            json={"chat_id": chat_id, "content": prompt}, timeout=30,
        )
        data = r.json() if r.content else {}
        for m in reversed(data.get("messages", [])):
            if m.get("role") == "agent":
                content = (m.get("content") or "").strip()
                return content or opening
    except Exception as exc:
        log(f"⚠️ Retell email completion error: {exc}")
    return opening

def extract_contact_details(text: str) -> Dict[str, str]:
    """Haal een bruikbare naam en e-mail uit een SMS zonder bestaande gegevens te overschrijven met ruis."""
    raw = (text or "").strip()
    result = {"name": "", "email": ""}
    if not raw:
        return result

    email_match = re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", raw, flags=re.IGNORECASE)
    if email_match:
        result["email"] = email_match.group(0).lower()
        remainder = (raw[:email_match.start()] + " " + raw[email_match.end():]).strip()
        remainder = re.sub(r"\b(en|and|naam|mijn naam is|ik ben|email|e-mail|mail)\b", " ", remainder, flags=re.IGNORECASE)
        remainder = re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ' -]+", " ", remainder)
        remainder = re.sub(r"\s+", " ", remainder).strip(" -,")
        words = [w for w in remainder.split() if len(w) > 1]
        if 2 <= len(words) <= 6:
            result["name"] = " ".join(words).title()
    return result


def update_conversation_contact(conversation_id: str, tenant_id: str, name: str = "", email: str = "") -> None:
    if not db_available() or not conversation_id or (not name and not email):
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE conversations
                    SET contact_name = COALESCE(NULLIF(%s, ''), contact_name),
                        contact_email = COALESCE(NULLIF(%s, ''), contact_email),
                        updated_at = NOW()
                    WHERE id = %s AND tenant_id = %s;
                    """,
                    ((name or "").strip(), (email or "").strip().lower(), conversation_id, tenant_id),
                )
    except Exception as exc:
        log(f"⚠️ update_conversation_contact failed: {exc}")


def reply_confirms_booking(text: str) -> bool:
    t = normalize_reply_for_compare(text)
    return any(phrase in t for phrase in [
        "ik heb het ingepland", "afspraak is ingepland", "afspraak werd ingepland",
        "je bent ingeschreven", "u bent ingeschreven", "bevestiging via e mail",
        "bevestiging per e mail", "afspraak is geboekt", "ik heb de afspraak geboekt"
    ])


def end_retell_chat(tenant: Dict[str, Any], phone: str) -> bool:
    key = (tenant["tenant_id"], phone)
    chat_id = SMS_SESSIONS.get(key)
    if not chat_id or not RETELL_API_KEY:
        SMS_SESSIONS.pop(key, None)
        return False
    try:
        response = requests.patch(
            f"{RETELL_BASE_URL}/end-chat/{chat_id}",
            headers={"Authorization": f"Bearer {RETELL_API_KEY}", "Content-Type": "application/json"},
            timeout=20,
        )
        ok = 200 <= response.status_code < 300
        if not ok:
            log(f"⚠️ Retell end-chat failed status={response.status_code} body={response.text[:250]}")
        return ok
    except Exception as exc:
        log(f"⚠️ Retell end-chat error: {exc}")
        return False
    finally:
        SMS_SESSIONS.pop(key, None)


def mark_conversation_completed(conversation_id: str, tenant_id: str) -> None:
    if not db_available() or not conversation_id:
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE conversations
                    SET status = 'afgesloten', requires_human = FALSE,
                        intent = 'afspraak_maken', urgency = 'normaal',
                        summary = 'De afspraak is succesvol ingepland.',
                        recommended_action = 'Geen verdere actie nodig. Het gesprek is afgerond.',
                        updated_at = NOW()
                    WHERE id = %s AND tenant_id = %s;
                    """,
                    (conversation_id, tenant_id),
                )
    except Exception as exc:
        log(f"⚠️ mark_conversation_completed failed: {exc}")

def is_ai_disabled_status(status: str) -> bool:
    s = (status or "").strip().lower().replace("-", "_").replace(" ", "_")
    return s in ("menselijke_overname", "human_required", "human_needed", "manual_takeover", "manual_overname", "ai_paused", "afgesloten", "closed")



def get_recent_conversation_messages(conversation_id: str, limit: int = 12) -> list:
    if not db_available() or not conversation_id:
        return []
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT direction, body, sender_type, created_at
                    FROM conversation_messages
                    WHERE conversation_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s;
                    """,
                    (conversation_id, int(limit)),
                )
                rows = cur.fetchall()
        return [
            {"direction": r[0], "body": r[1] or "", "sender_type": r[2] or "unknown", "created_at": r[3]}
            for r in reversed(rows)
        ]
    except Exception as e:
        log(f"⚠️ get_recent_conversation_messages failed: {e}")
        return []


def normalize_reply_for_compare(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def detect_ai_stall(conversation_id: str, proposed_reply: str) -> Optional[str]:
    """Detecteer herhaling of een gesprek dat niet vooruitgaat."""
    reply_key = normalize_reply_for_compare(proposed_reply)
    if not reply_key:
        return "De AI gaf geen bruikbaar antwoord."

    history = get_recent_conversation_messages(conversation_id, 14)
    ai_messages = [m for m in history if m.get("direction") == "outgoing" and m.get("sender_type") == "ai"]
    recent_keys = [normalize_reply_for_compare(m.get("body", "")) for m in ai_messages[-4:]]

    if reply_key in recent_keys:
        return "De AI herhaalt hetzelfde antwoord."

    # Dezelfde of vrijwel dezelfde vraag meerdere keren is een sterke aanwijzing dat de flow vastloopt.
    question_stems = []
    for m in ai_messages[-5:]:
        body = normalize_reply_for_compare(m.get("body", ""))
        if "?" in (m.get("body") or "") or any(x in body for x in ["ben je", "bent u", "welke dag", "welk uur", "wat kan ik"]):
            question_stems.append(body[:90])
    if len(question_stems) >= 2 and reply_key[:90] in question_stems:
        return "De AI stelt opnieuw dezelfde vraag en de conversatie gaat niet vooruit."

    return None


def mark_ai_takeover_needed(conversation_id: str, tenant_id: str, reason: str) -> None:
    if not db_available() or not conversation_id:
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE conversations
                    SET status = 'menselijke_overname',
                        requires_human = TRUE,
                        urgency = 'hoog',
                        intent = 'menselijke_overname',
                        summary = %s,
                        recommended_action = %s,
                        suggested_reply = %s,
                        updated_at = NOW()
                    WHERE id = %s AND tenant_id = %s;
                    """,
                    (
                        reason,
                        "Neem het gesprek over, controleer wat al gevraagd is en antwoord persoonlijk.",
                        "Dag, ik neem dit gesprek even persoonlijk over zodat ik u correct kan verderhelpen.",
                        conversation_id,
                        tenant_id,
                    ),
                )
    except Exception as e:
        log(f"⚠️ mark_ai_takeover_needed failed: {e}")

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
            details = extract_contact_details(text)
            if details.get("name") or details.get("email"):
                update_conversation_contact(conv["id"], tenant["tenant_id"], details.get("name", ""), details.get("email", ""))
            # Bouw samenvatting, urgentie en aanbevolen actie op uit de recente conversatie,
            # niet alleen uit het allerlaatste bericht.
            recent = get_recent_conversation_messages(conv["id"], 16)
            conversation_text = " ".join(m.get("body", "") for m in recent if m.get("body"))
            analysis = classify_text_basic(conversation_text or text)

            # Alleen een echte handmatige overname, expliciete overnamevraag of afgesloten gesprek blokkeert AI.
            # 'inactive' betekent enkel dat er nog geen activiteit was en mag een eerste antwoord nooit blokkeren.
            if is_ai_disabled_status(current_status):
                set_conversation_status(conv["id"], tenant["tenant_id"], current_status, True)
                log(f"🤝 AI disabled for conversation={conv['id']} status={current_status}; inbound saved only")
                return "OK", 200

            # Zodra een klant bericht en AI aan staat, is het gesprek actief.
            update_conversation_ai(conv["id"], analysis)
            if analysis.get("requiresHuman"):
                set_conversation_status(conv["id"], tenant["tenant_id"], "menselijke_overname", True)
                log(f"🤝 Human takeover required for conversation={conv['id']}; Retell skipped")
                return "OK", 200
            set_conversation_status(conv["id"], tenant["tenant_id"], "ai-active", False)

        reply = ask_retell_via_sms(tenant, sender, text)

        if conv:
            stall_reason = detect_ai_stall(conv["id"], reply)
            if stall_reason:
                mark_ai_takeover_needed(conv["id"], tenant["tenant_id"], stall_reason)
                log(f"⚠️ AI stall detected conversation={conv['id']}: {stall_reason}")
                return "OK", 200

        if reply:
            send_sms(tenant, sender, reply)

        if conv and reply:
            add_conversation_message(conv["id"], tenant["tenant_id"], "outgoing", reply, "sms", sender_type="ai")
            if reply_confirms_booking(reply):
                mark_conversation_completed(conv["id"], tenant["tenant_id"])
                end_retell_chat(tenant, sender)
                log(f"✅ Booking confirmed; conversation completed and Retell chat ended conversation={conv['id']}")
            else:
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
        conv = get_or_create_conversation(tenant, phone=caller, channel="sms")
        if conv:
            add_conversation_message(conv["id"], tenant["tenant_id"], "incoming", "Gemiste oproep", "sms", sender_type="system")
            update_conversation_ai(conv["id"], classify_text_basic("Gemiste oproep. Klant verwacht terugkoppeling."))
        else:
            log(f"❌ /call/missed: SMS can be sent, but conversation could not be stored tenant={tenant.get('tenant_id')} caller={normalize_phone(caller)} db_available={db_available()}")
        send_sms(tenant, caller, opening)
        if conv:
            add_conversation_message(conv["id"], tenant["tenant_id"], "outgoing", opening, "sms", sender_type="ai")

    return "OK", 200





@app.route("/conversations", methods=["GET", "PATCH", "POST", "DELETE"])
def conversations():
    tenant = get_tenant_from_request_or_default()
    if not db_available():
        return jsonify({"status": "success", "data": []}), 200
    if not tenant:
        return jsonify({"status": "error", "error": "Geen platformtenant geselecteerd. Stel REACTIFY_TENANT_ID correct in."}), 400
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
            channel = (body.get("channel") or "sms").strip().lower()
            subject = (body.get("subject") or "").strip()

            # POST zonder status = nieuw gesprek/contact aanmaken vanuit Reactify.
            if request.method == "POST" and not status and (phone or email or name):
                conv = get_or_create_conversation(tenant, phone=phone, name=name, email=email, channel=channel, subject=subject)
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
                            channel = COALESCE(NULLIF(%s, ''), channel),
                            subject = COALESCE(NULLIF(%s, ''), subject),
                            updated_at = NOW()
                        WHERE id = %s AND tenant_id = %s;
                        """,
                        (name.strip(), email.strip().lower(), normalized_phone, channel, subject, conversation_id, tenant["tenant_id"]),
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
                # Herstel een succesvolle afspraak altijd naar Afgerond. Dit vangt ook
                # oudere/local Cal.com-syncs op waarbij samenvatting en actie al correct
                # stonden, maar de status door een race condition nog 'ai-active' bleef.
                cur.execute(
                    """
                    UPDATE conversations
                    SET status = 'afgesloten', requires_human = FALSE
                    WHERE tenant_id = %s
                      AND updated_at >= NOW() - INTERVAL '30 minutes'
                      AND status NOT IN ('afgesloten', 'inactive', 'manual_overname', 'manual_takeover', 'overgenomen', 'taken_over', 'menselijke_overname', 'human_required', 'human_needed', 'overname_nodig')
                      AND (
                        LOWER(COALESCE(summary, '')) LIKE '%%afspraak%%succesvol%%ingepland%%'
                        OR LOWER(COALESCE(recommended_action, '')) LIKE '%%gesprek%%afgerond%%'
                      );
                    """,
                    (tenant["tenant_id"],),
                )

                # Herstel inconsistente overnamestatussen. Wanneer de analyse menselijke
                # opvolging vereist of expliciet adviseert het gesprek over te nemen, mag
                # de zichtbare status nooit op AI actief blijven staan.
                cur.execute(
                    """
                    UPDATE conversations
                    SET status = 'menselijke_overname', requires_human = TRUE
                    WHERE tenant_id = %s
                      AND status IN ('ai-active', 'ai_active', 'ai-actief', 'ai_actief', 'inactive')
                      AND (
                        requires_human = TRUE
                        OR LOWER(COALESCE(recommended_action, '')) LIKE '%%neem%%gesprek%%over%%'
                        OR LOWER(COALESCE(summary, '')) LIKE '%%menselijke opvolging%%'
                        OR LOWER(COALESCE(summary, '')) LIKE '%%medewerker%%'
                      );
                    """,
                    (tenant["tenant_id"],),
                )

                # Elke chat wordt na 30 minuten zonder activiteit inactief, ook afgeronde chats.
                cur.execute(
                    """
                    UPDATE conversations
                    SET status = 'inactive', requires_human = FALSE
                    WHERE tenant_id = %s AND updated_at < NOW() - INTERVAL '30 minutes'
                      AND status <> 'inactive';
                    """,
                    (tenant["tenant_id"],),
                )
                base_query = """
                    SELECT c.id, c.tenant_id, c.contact_phone, c.contact_email, c.contact_name,
                           c.channel, c.status, c.intent, c.urgency, c.requires_human, c.summary,
                           c.recommended_action, c.suggested_reply, c.subject, c.external_thread_id, c.created_at, c.updated_at,
                           lm.body AS last_message, lm.created_at AS last_message_at, lm.direction AS last_message_direction
                    FROM conversations c
                    LEFT JOIN LATERAL (
                      SELECT body, created_at, direction FROM conversation_messages m
                      WHERE m.conversation_id = c.id ORDER BY created_at DESC LIMIT 1
                    ) lm ON TRUE
                """
                cur.execute(base_query + " WHERE c.tenant_id = %s ORDER BY c.updated_at DESC LIMIT %s;", (tenant["tenant_id"], limit))
                rows = cur.fetchall()
        data = []
        for r in rows:
            data.append({
                "id": r[0], "tenant_id": r[1], "contact_phone": r[2] or "", "contact_email": r[3] or "",
                "contact_name": r[4] or r[2] or "Onbekende klant", "channel": r[5] or "sms", "status": r[6] or "ai-active",
                "intent": r[7] or "", "urgency": r[8] or "", "requires_human": bool(r[9]), "summary": r[10] or "",
                "recommended_action": r[11] or "", "suggested_reply": r[12] or "", "subject": r[13] or "", "external_thread_id": r[14] or "",
                "created_at": r[15].isoformat() if r[15] else None, "updated_at": r[16].isoformat() if r[16] else None,
                "last_message": r[17] or "", "last_message_at": r[18].isoformat() if r[18] else None, "last_message_direction": r[19] or "",
            })
        return jsonify({"status": "success", "data": data}), 200
    except Exception as e:
        log(f"❌ /conversations error: {e}")
        return jsonify({"status": "error", "error": "Kon gesprekken niet ophalen.", "details": str(e)}), 500


@app.route("/conversation-messages", methods=["GET"])
def conversation_messages():
    conversation_id = (request.args.get("conversationId") or request.args.get("conversation_id") or request.args.get("id") or "").strip()
    if not conversation_id:
        return jsonify({"status": "error", "error": "conversationId ontbreekt."}), 400
    if not db_available():
        return jsonify({"status": "success", "data": []}), 200
    tenant = get_conversation_tenant(conversation_id) or get_tenant_from_request_or_default()
    if not tenant:
        return jsonify({"status": "success", "data": []}), 200
    try:
        ensure_conversation_tables()
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT m.id, m.conversation_id, m.tenant_id, m.direction, m.channel, m.body, m.subject, m.html_body, m.external_id, m.external_thread_id, m.in_reply_to, m.created_at, m.sender_type
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
            "body": r[5], "text": r[5], "subject": r[6] or "", "html_body": r[7] or "", "external_id": r[8] or "", "external_thread_id": r[9] or "", "in_reply_to": r[10] or "", "created_at": r[11].isoformat() if r[11] else None, "sender_type": r[12] or ("customer" if r[3] == "incoming" else "unknown"),
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
        if not send_sms(tenant, normalize_phone(phone), message):
            return jsonify({"status": "error", "error": "SMSTools kon de SMS niet verzenden."}), 502
        msg_id = add_conversation_message(conversation_id, tenant_id, "outgoing", message, "sms", sender_type="manual")
        # Elk manueel bericht vanuit Reactify betekent: ondernemer heeft overgenomen, AI blijft uit.
        set_conversation_status(conversation_id, tenant_id, "manual_overname", True)
        return jsonify({"status": "success", "data": {"id": msg_id, "conversationId": conversation_id, "status": "manual_overname", "aiEnabled": False}}), 200
    except Exception as e:
        log(f"❌ /send-sms error: {e}")
        return jsonify({"status": "error", "error": "SMS verzenden mislukt.", "details": str(e)}), 500


@app.route("/email-settings", methods=["GET", "PATCH", "POST", "DELETE"])
def email_settings():
    tenant = get_tenant_from_request_or_default()
    if not tenant:
        return jsonify({"status": "error", "error": "Geen platformtenant geselecteerd."}), 400
    if not db_available():
        return jsonify({"status": "error", "error": "DATABASE_URL ontbreekt."}), 500
    ensure_conversation_tables()
    try:
        if request.method == "DELETE":
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM tenant_email_settings WHERE tenant_id = %s;", (tenant["tenant_id"],))
            return jsonify({"status": "success", "data": {"deleted": True}}), 200
        if request.method in ("PATCH", "POST"):
            body = request.get_json(force=True, silent=True) or {}
            current = get_email_settings(tenant["tenant_id"], include_password=False) or {}
            password = str(body.get("password") or "")
            encrypted = encrypt_email_secret(password) if password else None
            values = {
                "enabled": bool(body.get("enabled", current.get("enabled", False))),
                "email_address": str(body.get("emailAddress", current.get("emailAddress", ""))).strip().lower(),
                "sender_name": str(body.get("senderName", current.get("senderName", ""))).strip(),
                "imap_host": str(body.get("imapHost", current.get("imapHost", ""))).strip(),
                "imap_port": to_int_safe(body.get("imapPort", current.get("imapPort", 993)), 993),
                "imap_security": str(body.get("imapSecurity", current.get("imapSecurity", "ssl"))).strip().lower(),
                "smtp_host": str(body.get("smtpHost", current.get("smtpHost", ""))).strip(),
                "smtp_port": to_int_safe(body.get("smtpPort", current.get("smtpPort", 587)), 587),
                "smtp_security": str(body.get("smtpSecurity", current.get("smtpSecurity", "starttls"))).strip().lower(),
                "username": str(body.get("username", current.get("username", ""))).strip(),
                "signature": str(body.get("signature", current.get("signature", ""))).strip(),
                "auto_reply": bool(body.get("autoReply", current.get("autoReply", True))),
            }
            if values["enabled"] and (not values["email_address"] or not values["imap_host"] or not values["smtp_host"] or not values["username"]):
                return jsonify({"status": "error", "error": "Vul e-mailadres, IMAP, SMTP en gebruikersnaam in."}), 400
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO tenant_email_settings
                          (tenant_id, enabled, email_address, sender_name, imap_host, imap_port, imap_security,
                           smtp_host, smtp_port, smtp_security, username, password_encrypted, signature, auto_reply)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (tenant_id) DO UPDATE SET
                          enabled=EXCLUDED.enabled, email_address=EXCLUDED.email_address, sender_name=EXCLUDED.sender_name,
                          imap_host=EXCLUDED.imap_host, imap_port=EXCLUDED.imap_port, imap_security=EXCLUDED.imap_security,
                          smtp_host=EXCLUDED.smtp_host, smtp_port=EXCLUDED.smtp_port, smtp_security=EXCLUDED.smtp_security,
                          username=EXCLUDED.username,
                          password_encrypted=CASE WHEN EXCLUDED.password_encrypted <> '' THEN EXCLUDED.password_encrypted ELSE tenant_email_settings.password_encrypted END,
                          signature=EXCLUDED.signature, auto_reply=EXCLUDED.auto_reply, updated_at=NOW();
                    """, (tenant["tenant_id"], values["enabled"], values["email_address"], values["sender_name"],
                          values["imap_host"], values["imap_port"], values["imap_security"], values["smtp_host"],
                          values["smtp_port"], values["smtp_security"], values["username"], encrypted or "",
                          values["signature"], values["auto_reply"]))
        data = get_email_settings(tenant["tenant_id"], include_password=False) or {"enabled": False, "hasPassword": False}
        return jsonify({"status": "success", "data": data}), 200
    except Exception as exc:
        log(f"❌ /email-settings error: {exc}")
        return jsonify({"status": "error", "error": "E-mailinstellingen konden niet worden verwerkt.", "details": str(exc)}), 500


@app.route("/email/test", methods=["POST"])
def email_test():
    tenant = get_tenant_from_request_or_default()
    if not tenant:
        return jsonify({"status": "error", "error": "Geen platformtenant geselecteerd."}), 400
    try:
        settings = get_email_settings(tenant["tenant_id"], include_password=True)
        if not settings:
            return jsonify({"status": "error", "error": "Sla eerst de e-mailinstellingen op."}), 400
        imap = _imap_connect(settings)
        imap.select("INBOX")
        imap.logout()
        ok, _, error = send_email_message(tenant, settings["emailAddress"], "Reactify testmail", "De e-mailkoppeling met Reactify werkt correct.")
        if not ok:
            return jsonify({"status": "error", "error": "IMAP werkt, maar SMTP verzenden mislukte.", "details": error}), 502
        return jsonify({"status": "success", "data": {"ok": True}}), 200
    except Exception as exc:
        return jsonify({"status": "error", "error": "Verbindingstest mislukt.", "details": str(exc)}), 502


@app.route("/email/sync", methods=["POST"])
def email_sync():
    tenant = get_tenant_from_request_or_default()
    if not tenant:
        return jsonify({"status": "error", "error": "Geen platformtenant geselecteerd."}), 400
    try:
        return jsonify({"status": "success", "data": sync_incoming_email(tenant)}), 200
    except Exception as exc:
        log(f"❌ /email/sync error: {exc}")
        return jsonify({"status": "error", "error": "E-mailsynchronisatie mislukt.", "details": str(exc)}), 502


@app.route("/send-message", methods=["POST"])
def send_message():
    body = request.get_json(force=True, silent=True) or {}
    conversation_id = (body.get("conversationId") or body.get("conversation_id") or "").strip()
    message = (body.get("message") or body.get("text") or "").strip()
    requested_channel = (body.get("channel") or "").strip().lower()
    subject = (body.get("subject") or "").strip()
    if not conversation_id or not message:
        return jsonify({"status": "error", "error": "conversationId en bericht zijn verplicht."}), 400
    try:
        ensure_conversation_tables()
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT tenant_id, contact_phone, contact_email, channel, subject FROM conversations WHERE id = %s LIMIT 1;", (conversation_id,))
                row = cur.fetchone()
        if not row:
            return jsonify({"status": "error", "error": "Gesprek niet gevonden."}), 404
        tenant_id, phone, email_address, stored_channel, stored_subject = row
        tenant = TENANTS_BY_ID.get(tenant_id)
        if not tenant:
            return jsonify({"status": "error", "error": "Tenant niet gevonden."}), 404
        channel = requested_channel or stored_channel or "sms"
        if channel == "email":
            if not email_address:
                return jsonify({"status": "error", "error": "Geen e-mailadres gekoppeld aan dit gesprek."}), 400
            final_subject = subject or stored_subject or "Bericht van " + (tenant.get("company_name") or "Reactify")
            in_reply_to, references = _last_email_thread_headers(conversation_id)
            ok, external_id, error = send_email_message(tenant, email_address, final_subject, message, in_reply_to=in_reply_to, references=references)
            if not ok:
                return jsonify({"status": "error", "error": "E-mail verzenden mislukt.", "details": error}), 502
            msg_id = add_conversation_message(conversation_id, tenant_id, "outgoing", message, "email", external_id=external_id, sender_type="manual", subject=final_subject, in_reply_to=in_reply_to)
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE conversations SET channel='email', subject=%s WHERE id=%s;", (final_subject, conversation_id))
        else:
            if not phone:
                return jsonify({"status": "error", "error": "Geen telefoonnummer gekoppeld aan dit gesprek."}), 400
            if not send_sms(tenant, normalize_phone(phone), message):
                return jsonify({"status": "error", "error": "SMSTools kon de SMS niet verzenden."}), 502
            msg_id = add_conversation_message(conversation_id, tenant_id, "outgoing", message, "sms", sender_type="manual")
            channel = "sms"
        set_conversation_status(conversation_id, tenant_id, "manual_overname", True)
        return jsonify({"status": "success", "data": {"id": msg_id, "conversationId": conversation_id, "channel": channel, "status": "manual_overname", "aiEnabled": False}}), 200
    except Exception as exc:
        log(f"❌ /send-message error: {exc}")
        return jsonify({"status": "error", "error": "Bericht verzenden mislukt.", "details": str(exc)}), 500


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
ensure_privacy_settings_table()
run_retention_cleanup(force=True)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

