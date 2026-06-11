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
from concurrent.futures import ThreadPoolExecutor
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import parseaddr, formataddr, make_msgid, format_datetime
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
EMAIL_SYNC_STATE_LOCK = threading.Lock()
EMAIL_SYNC_RUNNING = set()
EMAIL_SYNC_LAST_RUN: Dict[str, datetime] = {}
EMAIL_SYNC_LAST_RESULT: Dict[str, Dict[str, Any]] = {}
EMAIL_REPLY_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="reactify-email-reply")
EMAIL_NETWORK_TIMEOUT = max(4, min(15, int(os.environ.get("EMAIL_NETWORK_TIMEOUT", "8"))))
APP_STARTED_AT = datetime.now(timezone.utc)
EMAIL_SYNC_EPOCH = (
    (os.environ.get("RENDER_GIT_COMMIT") or "").strip()
    or (os.environ.get("RENDER_DEPLOY_ID") or "").strip()
    or "reactify-email-clean-start-2026-06-11-v6"
)

_CONVERSATION_TABLES_READY = False
_CONVERSATION_TABLES_LOCK = threading.Lock()


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
    global _CONVERSATION_TABLES_READY
    if _CONVERSATION_TABLES_READY:
        return
    if not db_available():
        log("⚠️ DATABASE_URL missing; conversations disabled")
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = '8s';")
                cur.execute("SELECT pg_advisory_xact_lock(74201926);")
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
                      last_imap_uid BIGINT NOT NULL DEFAULT 0,
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    ALTER TABLE conversations ADD COLUMN IF NOT EXISTS folder TEXT NOT NULL DEFAULT 'inbox';
                    ALTER TABLE conversations ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
                    UPDATE conversations SET folder = 'inbox' WHERE folder IS NULL OR folder = '';
                    CREATE INDEX IF NOT EXISTS idx_conversations_folder ON conversations (tenant_id, folder, updated_at DESC);
                    ALTER TABLE tenant_email_settings ADD COLUMN IF NOT EXISTS last_imap_uid BIGINT NOT NULL DEFAULT 0;
                    ALTER TABLE tenant_email_settings ADD COLUMN IF NOT EXISTS sync_epoch TEXT NOT NULL DEFAULT '';
                    ALTER TABLE tenant_email_settings ADD COLUMN IF NOT EXISTS sync_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

                    CREATE TABLE IF NOT EXISTS email_import_tombstones (
                      tenant_id TEXT NOT NULL,
                      external_id TEXT NOT NULL,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      PRIMARY KEY (tenant_id, external_id)
                    );
                    CREATE TABLE IF NOT EXISTS email_auto_reply_claims (
                      tenant_id TEXT NOT NULL,
                      external_id TEXT NOT NULL,
                      conversation_id TEXT NOT NULL,
                      claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      sent_at TIMESTAMPTZ,
                      PRIMARY KEY (tenant_id, external_id)
                    );
                    """
                )
        _CONVERSATION_TABLES_READY = True
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


def is_generic_contact_name(value: str) -> bool:
    """Herken tijdelijke profielnamen die nooit een echte naam mogen overschrijven."""
    name = re.sub(r"\s+", " ", (value or "").strip()).lower()
    if not name:
        return True
    if "@" in name:
        return True
    return name in {
        "nieuwe lead", "nieuwe klant", "onbekende klant", "onbekend",
        "klant", "lead", "geen naam", "unknown", "new lead", "customer"
    }


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
                    # Een IMAP-header zoals "Nieuwe lead" mag een eerder herkende
                    # handtekeningnaam (bv. Chris Damian) nooit opnieuw overschrijven.
                    safe_name = "" if is_generic_contact_name(name) else (name or "").strip()
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
                    """, (safe_name, normalized_email, normalized_phone, normalized_channel, subject or "", external_thread_id or "", conv_id))
                    cur.execute("SELECT contact_name FROM conversations WHERE id = %s;", (conv_id,))
                    saved_name_row = cur.fetchone()
                    saved_name = (saved_name_row[0] if saved_name_row else "") or safe_name or name
                    return {"id": conv_id, "tenant_id": tenant_id, "contact_phone": normalized_phone, "contact_name": saved_name, "contact_email": normalized_email, "channel": normalized_channel, "subject": subject, "status": existing_status}
                conv_id = new_id("conv")
                display_name = name or (normalized_email if normalized_channel == "email" else phone) or "Onbekende klant"
                cur.execute("""
                    INSERT INTO conversations (id, tenant_id, contact_phone, contact_email, contact_name, channel, status, summary, subject, external_thread_id)
                    VALUES (%s, %s, %s, %s, %s, %s, 'ai-active', %s, %s, %s);
                """, (conv_id, tenant_id, normalized_phone, normalized_email, display_name, normalized_channel, "", subject or "", external_thread_id or ""))
                return {"id": conv_id, "tenant_id": tenant_id, "contact_phone": normalized_phone, "contact_name": display_name, "contact_email": normalized_email, "channel": normalized_channel, "subject": subject, "status": "ai-active", "folder": "inbox"}
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
                       signature, auto_reply, last_imap_uid, updated_at
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
        "autoReply": bool(row[12]), "lastImapUid": int(row[13] or 0), "updatedAt": row[14].isoformat() if row[14] else None,
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
    text_body = _clean_email_text(text_body)
    return text_body.strip(), html_body.strip()



def _clean_email_text(text: str) -> str:
    """Maak een e-mail leesbaar voor de Reactify-chat.

    Verwijdert tracking-URL's, technische linknotatie, unsubscribe-blokken,
    doorgestuurde headers en lange geciteerde antwoordgeschiedenis.
    """
    value = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    value = unescape(value)
    # Markdown/HTML-achtige links: label <url> en [label](url) -> label.
    value = re.sub(r"\[([^\]]+)\]\(https?://[^)]+\)", r"\1", value, flags=re.I)
    value = re.sub(r"<https?://[^>]+>", "", value, flags=re.I)
    # Losse, zeer lange trackinglinks verwijderen; korte normale links behouden.
    value = re.sub(r"https?://\S{90,}", "", value, flags=re.I)
    # Stop vóór geciteerde mailgeschiedenis of technische headers.
    cut_patterns = [
        r"(?im)^\s*On .+ wrote:\s*$", r"(?im)^\s*Op .+ schreef .+:\s*$",
        r"(?im)^\s*Van:\s+.+$", r"(?im)^\s*From:\s+.+$",
        r"(?im)^\s*-{2,}\s*(Original Message|Oorspronkelijk bericht)\s*-{2,}\s*$",
    ]
    cut = len(value)
    for pattern in cut_patterns:
        match = re.search(pattern, value)
        if match:
            cut = min(cut, match.start())
    value = value[:cut]
    # Nieuwsbrief- en privacyvoetregels niet in de chat tonen.
    footer = re.search(
        r"(?im)^\s*(unsubscribe|uitschrijven|manage (your )?email preferences|"
        r"privacy notice|bekijk in browser|view (this )?email in (your )?browser)\b.*$",
        value,
    )
    if footer:
        value = value[:footer.start()]
    lines = []
    for line in value.split("\n"):
        stripped = line.strip()
        if stripped.startswith(">"):
            continue
        # technische mailvelden en kale trackingregels overslaan
        if re.match(r"(?i)^(message-id|references|in-reply-to|content-type|mime-version):", stripped):
            continue
        lines.append(line.rstrip())
    value = "\n".join(lines)
    value = re.sub(r"[ \t]+\n", "\n", value)
    # Verwijder typische automatische AI-, tracking- en juridische voetnoten.
    noisy_patterns = [
        r"(?im)^.*powered by ai.*$",
        r"(?im)^.*generated (by|using) (an )?ai.*$",
        r"(?im)^.*this (email|message) was sent automatically.*$",
        r"(?im)^.*do not reply to this (email|message).*$",
        r"(?im)^.*privacy policy.*$",
        r"(?im)^.*terms (of use|and conditions).*$",
        r"(?im)^.*cookie policy.*$",
        r"(?im)^.*manage (your )?(email )?preferences.*$",
        r"(?im)^.*unsubscribe.*$",
    ]
    for pattern in noisy_patterns:
        value = re.sub(pattern, "", value)
    # Verwijder code-/templateblokken die geen normale e-mailtekst zijn.
    value = re.sub(r"(?s)```.*?```", "", value)
    value = re.sub(r"(?m)^\s*[{}\[\]<>]{2,}.*$", "", value)
    value = re.sub(r"(?m)^\s*(?:var|const|let|function|SELECT|INSERT|UPDATE|DELETE)\b.*$", "", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def _current_imap_uid(client) -> int:
    status, data = client.uid("search", None, "ALL")
    ids = (data[0] or b"").split() if status == "OK" and data else []
    try:
        return int(ids[-1]) if ids else 0
    except Exception:
        return 0


def _imap_uid_before_app_start(client) -> int:
    """Vind het laatste bericht dat al bestond vóór deze backend-deploy.

    Zo worden mails die na het starten van de nieuwe deploy aankomen niet
    overgeslagen, ook wanneer de eerste synchronisatie pas wat later gebeurt.
    """
    status, data = client.uid("search", None, "ALL")
    ids = (data[0] or b"").split() if status == "OK" and data else []
    if not ids:
        return 0

    baseline = 0
    # Alleen het recente einde van de mailbox hoeft onderzocht te worden.
    for uid_value in ids[-500:]:
        try:
            numeric_uid = int(uid_value)
        except Exception:
            continue
        status, rows = client.uid("fetch", uid_value, "(INTERNALDATE)")
        if status != "OK" or not rows:
            continue
        metadata = next((row[0] for row in rows if isinstance(row, tuple) and row), b"")
        if not isinstance(metadata, (bytes, bytearray)):
            continue
        match = re.search(rb'INTERNALDATE "([^"]+)"', metadata)
        if not match:
            continue
        try:
            received_at = datetime.strptime(match.group(1).decode("ascii"), "%d-%b-%Y %H:%M:%S %z")
        except Exception:
            continue
        if received_at < APP_STARTED_AT:
            baseline = max(baseline, numeric_uid)
    return baseline


def _apply_email_deploy_cutoff(tenant: Dict[str, Any], client) -> Tuple[bool, int]:
    """Begin per Render-deploy vanaf het deploymoment en wis oude mailimport."""
    tenant_id = tenant["tenant_id"]
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sync_epoch, last_imap_uid FROM tenant_email_settings WHERE tenant_id = %s FOR UPDATE;",
                (tenant_id,),
            )
            row = cur.fetchone()
            current_epoch = (row[0] or "") if row else ""
            current_uid = int(row[1] or 0) if row else 0
            if current_epoch == EMAIL_SYNC_EPOCH:
                return False, current_uid

            baseline_uid = _imap_uid_before_app_start(client)
            cur.execute(
                """
                UPDATE tenant_email_settings
                SET last_imap_uid = %s, sync_epoch = %s, sync_started_at = %s, updated_at = NOW()
                WHERE tenant_id = %s;
                """,
                (baseline_uid, EMAIL_SYNC_EPOCH, APP_STARTED_AT, tenant_id),
            )
            # Oude e-mailgesprekken en berichten verdwijnen uit Reactify; de mailbox zelf blijft onaangeroerd.
            cur.execute("DELETE FROM conversations WHERE tenant_id = %s AND channel = 'email';", (tenant_id,))
    log(f"✅ E-mailstartpunt ingesteld op deploymoment, baseline UID={baseline_uid} tenant={tenant_id}")
    return True, baseline_uid


def _normalized_subject(value: str) -> str:
    subject = re.sub(r"^(?:(?:re|fw|fwd)\s*:\s*)+", "", (value or "").strip(), flags=re.I)
    return subject[:500]


def _imap_connect(settings: Dict[str, Any]):
    host, port = settings["imapHost"], int(settings.get("imapPort") or 993)
    security = (settings.get("imapSecurity") or "ssl").lower()
    if security == "ssl":
        client = imaplib.IMAP4_SSL(
            host, port, ssl_context=ssl.create_default_context(), timeout=EMAIL_NETWORK_TIMEOUT
        )
    else:
        client = imaplib.IMAP4(host, port, timeout=EMAIL_NETWORK_TIMEOUT)
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
    msg["Date"] = format_datetime(datetime.now(timezone.utc))
    msg["Reply-To"] = sender_address
    external_id = make_msgid(domain=sender_address.split("@")[-1] if "@" in sender_address else None)
    msg["Message-ID"] = external_id
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
    if references:
        msg["References"] = references
    signature = (settings.get("signature") or "").strip()
    clean_body = body.strip()
    has_closing = bool(re.search(
        r"(?im)^\s*(?:met\s+)?(?:vriendelijke|hartelijke)\s+groet(?:en)?[,:]?\s*$",
        clean_body,
    ))
    full_body = clean_body if (not signature or has_closing) else clean_body + "\n\n" + signature
    msg.set_content(full_body)
    try:
        host, port = settings["smtpHost"], int(settings.get("smtpPort") or 587)
        security = (settings.get("smtpSecurity") or "starttls").lower()
        if security == "ssl":
            smtp = smtplib.SMTP_SSL(host, port, context=ssl.create_default_context(), timeout=EMAIL_NETWORK_TIMEOUT)
        else:
            smtp = smtplib.SMTP(host, port, timeout=EMAIL_NETWORK_TIMEOUT)
            smtp.ehlo()
            if security == "starttls":
                smtp.starttls(context=ssl.create_default_context())
                smtp.ehlo()
        smtp.login(settings.get("username") or sender_address, settings.get("password") or "")
        smtp.send_message(msg, from_addr=sender_address, to_addrs=[to_email])
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


def _read_conversation_status(conversation_id: str, tenant_id: str) -> str:
    if not db_available() or not conversation_id or not tenant_id:
        return ""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM conversations WHERE id = %s AND tenant_id = %s LIMIT 1;",
                    (conversation_id, tenant_id),
                )
                row = cur.fetchone()
        return (row[0] or "") if row else ""
    except Exception as exc:
        log(f"⚠️ Kon e-mailgespreksstatus niet lezen: {exc}")
        return ""


def _email_external_is_tombstoned(tenant_id: str, external_id: str) -> bool:
    if not tenant_id or not external_id or not db_available():
        return False
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM email_import_tombstones WHERE tenant_id = %s AND external_id = %s LIMIT 1;",
                    (tenant_id, external_id),
                )
                return bool(cur.fetchone())
    except Exception as exc:
        log(f"⚠️ Tombstonecontrole mislukt: {exc}")
        return False


def _claim_email_auto_reply(tenant_id: str, external_id: str, conversation_id: str) -> bool:
    if not tenant_id or not external_id or not conversation_id or not db_available():
        return False
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO email_auto_reply_claims (tenant_id, external_id, conversation_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING external_id;
                    """,
                    (tenant_id, external_id, conversation_id),
                )
                return bool(cur.fetchone())
    except Exception as exc:
        log(f"⚠️ E-mailantwoord claim mislukt: {exc}")
        return False


def _release_email_auto_reply_claim(tenant_id: str, external_id: str) -> None:
    if not tenant_id or not external_id or not db_available():
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM email_auto_reply_claims WHERE tenant_id = %s AND external_id = %s AND sent_at IS NULL;",
                    (tenant_id, external_id),
                )
    except Exception as exc:
        log(f"⚠️ E-mailantwoord claim vrijgeven mislukt: {exc}")


def _mark_email_auto_reply_sent(tenant_id: str, external_id: str) -> None:
    if not tenant_id or not external_id or not db_available():
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE email_auto_reply_claims SET sent_at = NOW() WHERE tenant_id = %s AND external_id = %s;",
                    (tenant_id, external_id),
                )
    except Exception as exc:
        log(f"⚠️ E-mailantwoord claim afronden mislukt: {exc}")


def _name_from_email_address(email_address: str) -> str:
    local = (email_address or "").split("@", 1)[0]
    local = re.sub(r"[._-]+", " ", local)
    local = re.sub(r"\d+", " ", local)
    parts = [p for p in local.split() if len(p) > 1]
    if not parts:
        return ""
    # Aaneengeschreven voor- en achternaam zoals christiandamian blijft moeilijk
    # betrouwbaar te splitsen; gebruik minstens nette titelkapitalisatie.
    return " ".join(p.capitalize() for p in parts[:4])


def ensure_ai_email_closing(text: str) -> str:
    """Geef elk automatisch antwoord exact dezelfde Reactify AI-afsluiting."""
    value = (text or "").strip()
    # Verwijder een reeds gegenereerde afsluiting om dubbele handtekeningen te voorkomen.
    value = re.sub(
        r"(?is)\n{1,3}\s*(?:met\s+)?(?:vriendelijke|hartelijke)\s+groet(?:en)?[,:]?\s*\n+.*$",
        "",
        value,
    ).strip()
    return value + "\n\nVriendelijke groet,\nReactify AI"


def get_name_from_recent_outgoing_email(conversation_id: str) -> str:
    """Lees een voornaam uit een recente aanhef zoals 'Hallo Christian,'."""
    if not db_available() or not conversation_id:
        return ""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT body FROM conversation_messages
                    WHERE conversation_id = %s AND channel = 'email' AND direction = 'outgoing'
                    ORDER BY created_at DESC LIMIT 5;
                    """,
                    (conversation_id,),
                )
                rows = cur.fetchall()
        for (body,) in rows:
            match = re.search(
                r"(?im)^\s*(?:hallo|dag|beste|goedemorgen|goedemiddag|goedenavond)\s+([A-ZÀ-ÖØ-Ý][A-Za-zÀ-ÖØ-öø-ÿ'’-]{1,30})(?:\s+[A-ZÀ-ÖØ-Ý][A-Za-zÀ-ÖØ-öø-ÿ'’-]{1,30})?[,!]?\s*$",
                (body or "") + "\n",
            )
            if match:
                name = match.group(1).strip()
                if name.lower() not in {"reactify", "klant", "team"}:
                    return name
    except Exception as exc:
        log(f"⚠️ Naam uit recente e-mailaanhef lezen mislukt: {exc}")
    return ""


def _process_email_auto_reply(
    tenant: Dict[str, Any], conversation_id: str, recipient: str, subject: str,
    text_body: str, external_id: str, references: str, external_thread_id: str
) -> None:
    """Maak en verstuur een AI-e-mail buiten de HTTP-request.

    IMAP-import blijft hierdoor snel en Netlify hoeft niet te wachten op Retell of SMTP.
    Vlak voor verzending wordt de actuele overnamestatus opnieuw gecontroleerd.
    """
    tenant_id = tenant.get("tenant_id") or ""
    try:
        current_status = _read_conversation_status(conversation_id, tenant_id)
        if is_ai_disabled_status(current_status):
            log(f"ℹ️ Automatisch e-mailantwoord overgeslagen: AI staat uit voor {conversation_id}")
            return

        settings = get_email_settings(tenant_id, include_password=False) or {}
        if not settings.get("enabled") or not settings.get("autoReply"):
            return

        reply = ask_retell_via_email(tenant, recipient, subject, text_body)
        if not reply:
            _release_email_auto_reply_claim(tenant_id, external_id)
            return
        reply = ensure_ai_email_closing(reply)
        reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"
        ok, sent_id, error = send_email_message(
            tenant, recipient, reply_subject, reply,
            in_reply_to=external_id,
            references=(references + " " + external_id).strip(),
        )
        if not ok:
            log(f"⚠️ Automatic email reply failed: {error}")
            _release_email_auto_reply_claim(tenant_id, external_id)
            return

        add_conversation_message(
            conversation_id, tenant_id, "outgoing", reply, "email",
            external_id=sent_id, sender_type="ai", subject=reply_subject,
            external_thread_id=external_thread_id, in_reply_to=external_id,
        )
        if reply_confirms_booking(reply):
            mark_conversation_completed(conversation_id, tenant_id)
            SMS_SESSIONS.pop((tenant_id, f"email:{recipient.strip().lower()}"), None)
            log(f"✅ Afspraak bevestigd via e-mail; gesprek afgerond={conversation_id}")
        else:
            set_conversation_status(conversation_id, tenant_id, "ai-active", False)
        _mark_email_auto_reply_sent(tenant_id, external_id)
        log(f"✅ Automatisch e-mailantwoord verzonden gesprek={conversation_id}")
    except Exception as exc:
        _release_email_auto_reply_claim(tenant_id, external_id)
        log(f"⚠️ Achtergrondtaak automatisch e-mailantwoord mislukt: {exc}")


def _email_is_spam_or_marketing(message, from_email: str, subject: str, text_body: str) -> bool:
    headers = ' '.join([str(message.get('Auto-Submitted') or ''), str(message.get('Precedence') or ''), str(message.get('List-Unsubscribe') or ''), str(message.get('List-Id') or ''), str(message.get('X-Spam-Flag') or ''), str(message.get('X-Spam-Status') or '')]).lower()
    haystack = (subject + '\n' + text_body[:2500]).lower()
    sender = (from_email or '').lower()
    hard_header = any(token in headers for token in ['yes', 'bulk', 'list-unsubscribe', 'auto-generated'])
    automated_sender = any(token in sender for token in ['no-reply', 'noreply', 'newsletter', 'marketing', 'mailer-daemon'])
    terms = ['unsubscribe', 'uitschrijven', 'nieuwsbrief', 'newsletter', 'promotie', 'promotion', 'korting', 'discount', 'aanbieding', 'sale', 'black friday', 'marketing', 'klik hier', 'view in browser', 'bekijk in browser', 'exclusieve deal']
    score = sum(1 for term in terms if term in haystack)
    return hard_header or (automated_sender and score >= 1) or score >= 3


def sync_incoming_email(tenant: Dict[str, Any], limit: int = 25) -> Dict[str, Any]:
    """Synchroniseer recente mailboxberichten via IMAP UID.

    De synchronisatie gebruikt een kleine UID-lookback. Daardoor worden berichten
    die tijdens een eerdere fout of deploy niet verwerkt raakten opnieuw bekeken,
    terwijl de unieke Message-ID-index dubbele opslag voorkomt. Zowel gelezen als
    ongelezen berichten worden opgehaald en de mailboxstatus wordt niet gewijzigd.
    """
    settings = get_email_settings(tenant["tenant_id"], include_password=True)
    if not settings or not settings.get("enabled"):
        return {"processed": 0, "replied": 0, "scanned": 0, "enabled": False}
    if not EMAIL_SYNC_LOCK.acquire(blocking=False):
        return {"processed": 0, "replied": 0, "scanned": 0, "busy": True, "enabled": True}

    processed = scanned = queued_replies = 0
    client = None
    saved_uid = 0
    highest_uid = 0
    try:
        client = _imap_connect(settings)
        status, _ = client.select("INBOX", readonly=True)
        if status != "OK":
            raise RuntimeError("De INBOX kon niet worden geopend.")

        initialized, baseline_uid = _apply_email_deploy_cutoff(tenant, client)
        if initialized:
            return {
                "processed": 0, "replied": 0, "queuedReplies": 0, "scanned": 0,
                "enabled": True, "initialized": True, "lastUid": baseline_uid,
            }
        settings = get_email_settings(tenant["tenant_id"], include_password=True) or settings
        saved_uid = int(settings.get("lastImapUid") or baseline_uid or 0)
        highest_uid = saved_uid

        # Importeer uitsluitend berichten die NA het opgeslagen startpunt zijn aangekomen.
        # Geen UID-lookback: anders kunnen oude mails na een deploy opnieuw verschijnen.
        if saved_uid > 0:
            status, data = client.uid("search", None, f"UID {saved_uid + 1}:*")
        else:
            status, data = client.uid("search", None, "ALL")
            initial_ids = (data[0] or b"").split() if status == "OK" and data else []
            baseline_uid = int(initial_ids[-1]) if initial_ids else 0
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE tenant_email_settings SET last_imap_uid = %s WHERE tenant_id = %s;", (baseline_uid, tenant["tenant_id"]))
            return {"processed": 0, "replied": 0, "scanned": 0, "enabled": True, "initialized": True, "lastUid": baseline_uid}
        ids = (data[0] or b"").split() if status == "OK" and data else []
        ids = ids[-max(1, min(int(limit or 50) * 2, 150)):]

        for uid_value in ids:
            try:
                numeric_uid = int(uid_value)
            except Exception:
                continue

            status, rows = client.uid("fetch", uid_value, "(BODY.PEEK[])")
            if status != "OK" or not rows:
                continue
            raw = next((row[1] for row in rows if isinstance(row, tuple) and len(row) > 1), None)
            if not raw:
                continue

            scanned += 1
            highest_uid = max(highest_uid, numeric_uid)
            message = BytesParser(policy=policy.default).parsebytes(raw)
            from_name, from_email = parseaddr(str(message.get("From") or ""))
            from_email = from_email.strip().lower()
            own_email = (settings.get("emailAddress") or "").strip().lower()
            if not from_email or from_email == own_email:
                continue

            external_id = str(message.get("Message-ID") or "").strip() or f"imap-uid:{numeric_uid}"
            if _email_external_is_tombstoned(tenant["tenant_id"], external_id):
                continue
            subject = str(message.get("Subject") or "").strip() or "Zonder onderwerp"
            normalized_subject = _normalized_subject(subject)
            in_reply_to = str(message.get("In-Reply-To") or "").strip()
            references = str(message.get("References") or "").strip()
            external_thread_id = in_reply_to or (references.split()[0] if references else "") or normalized_subject
            text_body, html_body = _email_bodies(message)
            if not text_body:
                continue
            target_folder = "spam" if _email_is_spam_or_marketing(message, from_email, subject, text_body) else "inbox"

            inferred_name = from_name or _name_from_email_address(from_email) or from_email
            conv = get_or_create_conversation(
                tenant,
                name=inferred_name,
                email=from_email,
                channel="email",
                subject=normalized_subject,
                external_thread_id=external_thread_id,
            )
            if not conv:
                log(f"⚠️ E-mailgesprek kon niet worden aangemaakt uid={numeric_uid}")
                continue
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """UPDATE conversations
                           SET folder = %s, deleted_at = NULL,
                               status = CASE WHEN %s = 'spam' THEN 'inactive' ELSE status END,
                               requires_human = CASE WHEN %s = 'spam' THEN FALSE ELSE requires_human END
                           WHERE id = %s AND tenant_id = %s;""",
                        (target_folder, target_folder, target_folder, conv["id"], tenant["tenant_id"]),
                    )

            inserted = add_conversation_message(
                conv["id"], tenant["tenant_id"], "incoming", text_body, "email",
                external_id=external_id, sender_type="customer", subject=subject,
                html_body=html_body, external_thread_id=external_thread_id,
                in_reply_to=in_reply_to,
            )
            if not inserted:
                # Reeds geïmporteerd; dit is normaal door de UID-lookback.
                continue

            # Wanneer de afzenderheader geen bruikbare naam bevat, haal de naam uit
            # bijvoorbeeld "Met vriendelijke groeten,\nVoornaam Achternaam".
            signature_name = extract_name_from_email_signature(text_body)
            context_name = get_name_from_recent_outgoing_email(conv["id"])
            detected_name = signature_name or context_name
            if detected_name:
                update_email_contact_name_from_signature(
                    conv["id"], tenant["tenant_id"], detected_name
                )
                conv["contact_name"] = detected_name

            processed += 1
            analysis = classify_text_basic(subject + "\n" + text_body)
            current_status = conv.get("status") or "ai-active"
            update_conversation_ai(conv["id"], analysis)

            if target_folder == "spam" or is_ai_disabled_status(current_status) or analysis.get("requiresHuman") or not settings.get("autoReply"):
                if analysis.get("requiresHuman"):
                    set_conversation_status(conv["id"], tenant["tenant_id"], "menselijke_overname", True)
                continue

            # Retell + SMTP mogen een Netlify-request nooit blokkeren.
            # De inkomende e-mail staat al veilig in PostgreSQL; het antwoord loopt in de achtergrond.
            if _claim_email_auto_reply(tenant["tenant_id"], external_id, conv["id"]):
                EMAIL_REPLY_EXECUTOR.submit(
                    _process_email_auto_reply, tenant.copy(), conv["id"], from_email, subject,
                    text_body, external_id, references, external_thread_id,
                )
                queued_replies += 1
            else:
                log(f"ℹ️ Automatisch antwoord al geclaimd/verzonden voor {external_id}")

        if highest_uid > saved_uid:
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE tenant_email_settings SET last_imap_uid = %s WHERE tenant_id = %s;",
                        (highest_uid, tenant["tenant_id"]),
                    )
        result = {
            "processed": processed,
            "replied": 0,
            "queuedReplies": queued_replies,
            "scanned": scanned,
            "enabled": True,
            "lastUid": highest_uid,
        }
        EMAIL_SYNC_LAST_RESULT[tenant["tenant_id"]] = result
        return result
    finally:
        try:
            if client:
                client.logout()
        except Exception:
            pass
        EMAIL_SYNC_LOCK.release()


def _email_sync_worker(tenant: Dict[str, Any]) -> None:
    tenant_id = tenant.get("tenant_id") or ""
    try:
        result = sync_incoming_email(tenant, limit=25)
        EMAIL_SYNC_LAST_RESULT[tenant_id] = result
    except Exception as exc:
        log(f"⚠️ Achtergrondsync inkomende e-mail mislukt: {exc}")
        EMAIL_SYNC_LAST_RESULT[tenant_id] = {
            "processed": 0, "replied": 0, "queuedReplies": 0,
            "enabled": True, "error": str(exc),
        }
    finally:
        with EMAIL_SYNC_STATE_LOCK:
            EMAIL_SYNC_RUNNING.discard(tenant_id)


def schedule_email_sync(
    tenant: Dict[str, Any], min_interval_seconds: int = 20, force: bool = False
) -> Dict[str, Any]:
    """Plan IMAP-sync en antwoord onmiddellijk aan de webrequest.

    Dit voorkomt Netlify 504 Inactivity Timeout. Slechts één sync per tenant kan tegelijk lopen.
    """
    tenant_id = tenant.get("tenant_id") or ""
    settings = get_email_settings(tenant_id, include_password=False)
    if not settings or not settings.get("enabled"):
        return {"processed": 0, "replied": 0, "queuedReplies": 0, "enabled": False}

    now_utc = datetime.now(timezone.utc)
    with EMAIL_SYNC_STATE_LOCK:
        if tenant_id in EMAIL_SYNC_RUNNING:
            previous = dict(EMAIL_SYNC_LAST_RESULT.get(tenant_id, {}))
            return {**previous, "enabled": True, "busy": True, "queued": False}

        last_run = EMAIL_SYNC_LAST_RUN.get(tenant_id)
        if not force and last_run and (now_utc - last_run).total_seconds() < min_interval_seconds:
            previous = dict(EMAIL_SYNC_LAST_RESULT.get(tenant_id, {}))
            return {**previous, "enabled": True, "throttled": True, "queued": False}

        EMAIL_SYNC_LAST_RUN[tenant_id] = now_utc
        EMAIL_SYNC_RUNNING.add(tenant_id)

    threading.Thread(
        target=_email_sync_worker,
        args=(tenant.copy(),),
        daemon=True,
        name=f"email-sync-{tenant_id[:18]}",
    ).start()
    previous = dict(EMAIL_SYNC_LAST_RESULT.get(tenant_id, {}))
    return {**previous, "enabled": True, "queued": True, "busy": False}


def maybe_sync_incoming_email(tenant: Dict[str, Any], min_interval_seconds: int = 30) -> Dict[str, Any]:
    """Backwards-compatible niet-blokkerende wrapper voor bestaande codepaden."""
    return schedule_email_sync(tenant, min_interval_seconds=min_interval_seconds, force=False)

def get_contact_context(tenant_id: str, contact_key: str) -> Dict[str, str]:
    """Bouw veilige Retell-context uit het bestaande Reactify-profiel en recente gesprekken."""
    context = {
        "customer_name": "",
        "customer_email": "",
        "customer_phone": "",
        "conversation_summary": "",
        "recent_conversation": "",
    }
    if not db_available() or not tenant_id or not contact_key:
        return context

    is_email = str(contact_key).startswith("email:")
    email = str(contact_key).split(":", 1)[1].strip().lower() if is_email else ""
    phone = "" if is_email else normalize_phone(contact_key)
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                if is_email:
                    cur.execute("""
                        SELECT id, contact_name, contact_email, contact_phone, summary
                        FROM conversations
                        WHERE tenant_id = %s AND LOWER(COALESCE(contact_email, '')) = %s
                        ORDER BY updated_at DESC LIMIT 1;
                    """, (tenant_id, email))
                else:
                    cur.execute("""
                        SELECT id, contact_name, contact_email, contact_phone, summary
                        FROM conversations
                        WHERE tenant_id = %s AND contact_phone = %s
                        ORDER BY updated_at DESC LIMIT 1;
                    """, (tenant_id, phone))
                row = cur.fetchone()
                if not row:
                    context["customer_email"] = email
                    context["customer_phone"] = phone
                    return context

                conversation_id, name, stored_email, stored_phone, summary = row
                cur.execute("""
                    SELECT direction, body FROM conversation_messages
                    WHERE conversation_id = %s
                    ORDER BY created_at DESC LIMIT 10;
                """, (conversation_id,))
                recent = list(reversed(cur.fetchall()))

        context.update({
            "customer_name": (name or "").strip(),
            "customer_email": (stored_email or email or "").strip().lower(),
            "customer_phone": (stored_phone or phone or "").strip(),
            "conversation_summary": (summary or "").strip(),
            "recent_conversation": "\n".join(
                f"{'Klant' if direction == 'incoming' else 'Reactify'}: {(body or '').strip()}"
                for direction, body in recent if (body or '').strip()
            )[-5000:],
        })
    except Exception as exc:
        log(f"⚠️ get_contact_context failed: {exc}")
    return context


def get_or_create_chat_id(tenant: Dict[str, Any], contact_key: str) -> Optional[str]:
    """Maak of hergebruik één Retell-chatsessie per tenant en contact/kanaal."""
    key = (tenant["tenant_id"], contact_key)
    if key in SMS_SESSIONS:
        return SMS_SESSIONS[key]
    if not RETELL_API_KEY or not tenant.get("retell_agent_id"):
        return None
    context = get_contact_context(tenant["tenant_id"], contact_key)
    is_email = str(contact_key).startswith("email:")
    try:
        response = requests.post(
            f"{RETELL_BASE_URL}/create-chat",
            headers={"Authorization": f"Bearer {RETELL_API_KEY}", "Content-Type": "application/json"},
            json={
                "agent_id": tenant["retell_agent_id"],
                "metadata": {"contact": contact_key, "channel": "email" if is_email else "sms"},
                "retell_llm_dynamic_variables": {
                    **context,
                    "customer_phone": "" if is_email else normalize_phone(contact_key),
                    "customer_email": str(contact_key).split(":", 1)[1] if is_email else context.get("customer_email", ""),
                    "channel": "email" if is_email else "sms",
                },
            },
            timeout=EMAIL_NETWORK_TIMEOUT,
        )
        data = response.json() if response.content else {}
        if not response.ok:
            log(f"⚠️ Retell create-chat failed status={response.status_code}: {str(data)[:300]}")
            return None
        chat_id = data.get("chat_id") or data.get("id")
        if chat_id:
            SMS_SESSIONS[key] = chat_id
            return chat_id
    except Exception as exc:
        log(f"⚠️ Retell create-chat error: {exc}")
    return None


def ask_retell_via_email(tenant: Dict[str, Any], email_address: str, subject: str, text: str) -> str:
    opening = tenant.get("opening_line") or "Bedankt voor uw e-mail."
    if not RETELL_API_KEY or not tenant.get("retell_agent_id"):
        return opening
    session_key = f"email:{email_address.strip().lower()}"
    chat_id = get_or_create_chat_id(tenant, session_key)
    if not chat_id:
        return opening
    prompt = (
        "Je antwoordt nu uitsluitend via e-mail. Schrijf een professionele, natuurlijke e-mail in het Nederlands. "
        "Gebruik geen SMS-afkortingen. Voeg geen onderwerpregel toe in de tekst en herhaal de volledige e-mail niet. "
        "Wanneer de klant een concrete vraag stelt, begin je antwoord exact met: "
        "'Bedankt om contact op te nemen met Reactify, ik ben de virtuele assistent.' "
        "Beantwoord daarna meteen en inhoudelijk de vraag van de klant. "
        "Vraag niet opnieuw waarmee je kunt helpen wanneer de vraag al duidelijk is. "
        "Geef geen algemene welkomstboodschap als vervanging voor het echte antwoord. "
        "Wanneer de klant geen vraag stelt, reageer je passend op de inhoud zonder deze verplichte openingszin. "
        "Sluit zelf niet af; Reactify voegt automatisch de vaste afsluiting toe.\n\n"
        f"Onderwerp: {subject}\nBericht van klant:\n{text}"
    )
    try:
        r = requests.post(
            f"{RETELL_BASE_URL}/create-chat-completion",
            headers={"Authorization": f"Bearer {RETELL_API_KEY}", "Content-Type": "application/json"},
            json={"chat_id": chat_id, "content": prompt}, timeout=12,
        )
        data = r.json() if r.content else {}
        for m in reversed(data.get("messages", [])):
            if m.get("role") == "agent":
                content = (m.get("content") or "").strip()
                return content or opening
    except Exception as exc:
        log(f"⚠️ Retell email completion error: {exc}")
    return opening

def extract_name_from_email_signature(text: str) -> str:
    """Herken een persoonsnaam direct onder een gebruikelijke e-mailafsluiting."""
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw.strip():
        return ""
    lines = [re.sub(r"\s+", " ", line).strip(" \t,;:-") for line in raw.split("\n")]
    closing = re.compile(
        r"^(?:met\s+)?(?:vriendelijke|hartelijke)\s+groet(?:en)?$|"
        r"^groet(?:en)?$|^mvg$|^kind\s+regards$|^best\s+regards$|"
        r"^regards$|^sincerely$|^cordialement$|^bien\s+à\s+vous$",
        re.IGNORECASE,
    )
    blocked = {
        "team", "reactify", "klantendienst", "customer service", "support",
        "administratie", "sales", "marketing", "directie", "bedrijf",
    }
    for index, line in enumerate(lines):
        if not closing.match(line):
            continue
        for candidate in lines[index + 1:index + 4]:
            if not candidate:
                continue
            if "@" in candidate or re.search(r"https?://|www\.|\d{4,}", candidate, re.I):
                continue
            cleaned = re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ' .-]", "", candidate).strip(" .-")
            words = [w for w in cleaned.split() if w]
            if not 1 <= len(words) <= 5 or len(cleaned) < 2 or len(cleaned) > 70:
                continue
            if cleaned.lower() in blocked:
                continue
            if any(w.lower() in {"bv", "bvba", "nv", "vzw", "inc", "ltd", "company"} for w in words):
                continue
            return " ".join(word[:1].upper() + word[1:] for word in words)
    return ""


def update_email_contact_name_from_signature(conversation_id: str, tenant_id: str, signature_name: str) -> None:
    """Vervang alleen een generieke e-mailnaam door de naam uit de handtekening."""
    name = (signature_name or "").strip()
    if not db_available() or not conversation_id or not tenant_id or not name:
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE conversations
                    SET contact_name = %s, updated_at = NOW()
                    WHERE id = %s AND tenant_id = %s
                      AND (
                        contact_name IS NULL OR BTRIM(contact_name) = '' OR
                        LOWER(contact_name) IN ('nieuwe lead', 'onbekende klant', 'klant') OR
                        LOWER(contact_name) = LOWER(COALESCE(contact_email, '')) OR
                        contact_name LIKE '%%@%%' OR
                        LOWER(REGEXP_REPLACE(contact_name, '[^a-z0-9]', '', 'g')) =
                        LOWER(REGEXP_REPLACE(SPLIT_PART(COALESCE(contact_email, ''), '@', 1), '[^a-z0-9]', '', 'g')) OR
                        LOWER(%s) LIKE LOWER(BTRIM(contact_name)) || ' %%'
                      );
                """, (name, conversation_id, tenant_id, name))
    except Exception as exc:
        log(f"⚠️ Naam uit e-mailhandtekening opslaan mislukt: {exc}")


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

        # De inboxpoll mag nooit op IMAP, Retell of SMTP wachten.
        # Plan hoogstens een achtergrondsync; de gesprekkenlijst antwoordt direct.
        if request.method == "GET":
            schedule_email_sync(tenant, min_interval_seconds=30, force=False)

        if request.method == "DELETE":
            body = request.get_json(force=True, silent=True) or {}
            action = (body.get("action") or request.args.get("action") or "permanent").strip().lower()
            conversation_id = (body.get("conversationId") or body.get("conversation_id") or request.args.get("conversationId") or request.args.get("id") or "").strip()
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    if action == "empty-trash":
                        cur.execute("DELETE FROM conversations WHERE tenant_id = %s AND folder = 'trash' RETURNING id;", (tenant["tenant_id"],))
                        ids = [r[0] for r in cur.fetchall()]
                        return jsonify({"status": "success", "data": {"emptied": len(ids), "ids": ids}}), 200
                    if not conversation_id:
                        return jsonify({"status": "error", "error": "conversationId ontbreekt."}), 400
                    # Onthoud alle inkomende e-mail-ID's vóór definitief verwijderen.
                    # Zo wordt een nog aanwezige mail op IMAP nooit opnieuw geïmporteerd.
                    cur.execute(
                        """
                        INSERT INTO email_import_tombstones (tenant_id, external_id)
                        SELECT tenant_id, external_id
                        FROM conversation_messages
                        WHERE conversation_id = %s AND tenant_id = %s
                          AND channel = 'email' AND direction = 'incoming'
                          AND COALESCE(external_id, '') <> ''
                        ON CONFLICT DO NOTHING;
                        """,
                        (conversation_id, tenant["tenant_id"]),
                    )
                    cur.execute("DELETE FROM conversations WHERE id = %s AND tenant_id = %s RETURNING id;", (conversation_id, tenant["tenant_id"]))
                    ids = [r[0] for r in cur.fetchall()]
            return jsonify({"status": "success", "data": {"deleted": True, "soft": False, "ids": ids}}), 200

        if request.method in ("PATCH", "POST"):
            body = request.get_json(force=True, silent=True) or {}
            conversation_id = (body.get("conversationId") or body.get("conversation_id") or request.args.get("conversationId") or request.args.get("id") or "").strip()
            status = (body.get("status") or body.get("conversationStatus") or "").strip()
            phone = body.get("phone") or body.get("contact_phone") or body.get("to") or ""
            name = body.get("name") or body.get("contact_name") or body.get("customerName") or ""
            email = body.get("email") or body.get("contact_email") or body.get("customerEmail") or ""
            requested_channel = (body.get("channel") or "").strip().lower()
            channel = requested_channel or ("sms" if request.method == "POST" else "")
            subject = (body.get("subject") or "").strip()
            folder = (body.get("folder") or "").strip().lower()
            if folder not in ("", "inbox", "spam"):
                return jsonify({"status": "error", "error": "Ongeldige map."}), 400

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
                            folder = COALESCE(NULLIF(%s, ''), folder),
                            status = CASE WHEN %s = 'spam' THEN 'inactive' ELSE status END,
                            requires_human = CASE WHEN %s = 'spam' THEN FALSE ELSE requires_human END,
                            deleted_at = CASE WHEN %s IN ('inbox','spam') THEN NULL ELSE deleted_at END,
                            updated_at = NOW()
                        WHERE id = %s AND tenant_id = %s;
                        """,
                        (name.strip(), email.strip().lower(), normalized_phone, channel, subject, folder, folder, folder, folder, conversation_id, tenant["tenant_id"]),
                    )

            if status:
                requires_human = status.strip().lower().replace("-", "_") not in ("ai_active", "ai_actief")
                set_conversation_status(conversation_id, tenant["tenant_id"], status, requires_human)
            else:
                requires_human = None

            return jsonify({"status": "success", "data": {"id": conversation_id, "status": status or None, "aiEnabled": None if requires_human is None else not requires_human}}), 200

        limit = max(1, min(500, to_int_safe(request.args.get("limit"), 100)))
        requested_folder = (request.args.get("folder") or "inbox").strip().lower()
        if requested_folder not in ("inbox", "spam", "trash", "all"):
            requested_folder = "inbox"
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
                           c.recommended_action, c.suggested_reply, c.subject, c.external_thread_id, c.folder, c.deleted_at, c.created_at, c.updated_at,
                           lm.body AS last_message, lm.created_at AS last_message_at, lm.direction AS last_message_direction
                    FROM conversations c
                    LEFT JOIN tenant_email_settings tes ON tes.tenant_id = c.tenant_id
                    LEFT JOIN LATERAL (
                      SELECT body, created_at, direction FROM conversation_messages m
                      WHERE m.conversation_id = c.id ORDER BY created_at DESC LIMIT 1
                    ) lm ON TRUE
                """
                cutoff_clause = " AND (c.channel <> 'email' OR tes.sync_started_at IS NULL OR c.created_at >= tes.sync_started_at)"
                if requested_folder == "all":
                    cur.execute(base_query + " WHERE c.tenant_id = %s" + cutoff_clause + " ORDER BY c.updated_at DESC LIMIT %s;", (tenant["tenant_id"], limit))
                else:
                    cur.execute(base_query + " WHERE c.tenant_id = %s AND c.folder = %s" + cutoff_clause + " ORDER BY c.updated_at DESC LIMIT %s;", (tenant["tenant_id"], requested_folder, limit))
                rows = cur.fetchall()
        data = []
        for r in rows:
            data.append({
                "id": r[0], "tenant_id": r[1], "contact_phone": r[2] or "", "contact_email": r[3] or "",
                "contact_name": r[4] or r[2] or "Onbekende klant", "channel": r[5] or "sms", "status": r[6] or "ai-active",
                "intent": r[7] or "", "urgency": r[8] or "", "requires_human": bool(r[9]), "summary": r[10] or "",
                "recommended_action": r[11] or "", "suggested_reply": r[12] or "", "subject": r[13] or "", "external_thread_id": r[14] or "",
                "folder": r[15] or "inbox", "deleted_at": r[16].isoformat() if r[16] else None,
                "created_at": r[17].isoformat() if r[17] else None, "updated_at": r[18].isoformat() if r[18] else None,
                "last_message": r[19] or "", "last_message_at": r[20].isoformat() if r[20] else None, "last_message_direction": r[21] or "",
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
    """Test IMAP en SMTP zonder een testmail te versturen.

    De vorige test verstuurde ook een bericht naar de eigen mailbox. Daardoor kon
    de Netlify-proxy op tragere mailservers een timeout/HTTP 500 tonen, terwijl de
    koppeling zelf al correct opgeslagen was. Deze test controleert beide logins
    afzonderlijk en geeft een gerichte fout terug.
    """
    tenant = get_tenant_from_request_or_default()
    if not tenant:
        return jsonify({"status": "error", "error": "Geen platformtenant geselecteerd."}), 400

    settings = get_email_settings(tenant["tenant_id"], include_password=True)
    if not settings:
        return jsonify({"status": "error", "error": "Sla eerst de e-mailinstellingen op."}), 400
    if not settings.get("password"):
        return jsonify({"status": "error", "error": "Er is geen mailboxwachtwoord opgeslagen."}), 400

    result = {"imap": False, "smtp": False}

    try:
        imap = _imap_connect(settings)
        status, _ = imap.select("INBOX", readonly=True)
        result["imap"] = status == "OK"
        try:
            imap.logout()
        except Exception:
            pass
    except Exception as exc:
        return jsonify({
            "status": "error",
            "error": "IMAP-verbinding mislukt.",
            "details": str(exc),
            "data": result,
        }), 502

    smtp = None
    try:
        host = settings["smtpHost"]
        port = int(settings.get("smtpPort") or 587)
        security = (settings.get("smtpSecurity") or "starttls").lower()
        if security == "ssl":
            smtp = smtplib.SMTP_SSL(host, port, context=ssl.create_default_context(), timeout=12)
        else:
            smtp = smtplib.SMTP(host, port, timeout=12)
            smtp.ehlo()
            if security == "starttls":
                smtp.starttls(context=ssl.create_default_context())
                smtp.ehlo()
        smtp.login(settings.get("username") or settings.get("emailAddress"), settings.get("password") or "")
        code, _ = smtp.noop()
        result["smtp"] = int(code) < 400
    except Exception as exc:
        return jsonify({
            "status": "error",
            "error": "IMAP werkt, maar SMTP-verbinding mislukt.",
            "details": str(exc),
            "data": result,
        }), 502
    finally:
        if smtp is not None:
            try:
                smtp.quit()
            except Exception:
                pass

    return jsonify({"status": "success", "data": {**result, "ok": True}}), 200


@app.route("/email/sync", methods=["GET", "POST"])
def email_sync():
    tenant = get_tenant_from_request_or_default()
    if not tenant:
        return jsonify({"status": "error", "error": "Geen platformtenant geselecteerd."}), 400
    try:
        body = request.get_json(force=False, silent=True) or {}
        force = str(request.args.get("force") or body.get("force") or "").lower() in ("1", "true", "yes")
        result = schedule_email_sync(tenant, min_interval_seconds=20, force=force)
        return jsonify({"status": "success", "data": result}), 202 if result.get("queued") else 200
    except Exception as exc:
        log(f"❌ /email/sync error: {exc}")
        return jsonify({"status": "error", "error": "E-mailsynchronisatie kon niet worden gestart.", "details": str(exc)}), 502


@app.route("/send-message", methods=["POST"])
@app.route("/messages/send", methods=["POST"])
def send_message():
    body = request.get_json(force=True, silent=True) or {}
    conversation_id = (body.get("conversationId") or body.get("conversation_id") or "").strip()
    message = (body.get("message") or body.get("text") or "").strip()
    requested_channel = (body.get("channel") or "").strip().lower()
    subject = (body.get("subject") or "").strip()
    payload_name = (body.get("name") or body.get("contact_name") or "").strip()
    payload_phone = normalize_phone(body.get("phone") or body.get("contact_phone") or "")
    payload_email = (body.get("email") or body.get("contact_email") or "").strip().lower()
    if not message:
        return jsonify({"status": "error", "error": "Bericht ontbreekt."}), 400
    if requested_channel and requested_channel not in ("sms", "email"):
        return jsonify({"status": "error", "error": "Onbekend communicatiekanaal."}), 400

    try:
        ensure_conversation_tables()
        row = None
        if conversation_id:
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT tenant_id, contact_phone, contact_email, channel, subject FROM conversations WHERE id = %s LIMIT 1;",
                        (conversation_id,),
                    )
                    row = cur.fetchone()

        # Een lokaal gesprek kan nog een oud backend-ID bevatten na een deploy of reset.
        # Maak in dat geval automatisch opnieuw het juiste backendgesprek aan.
        if not row:
            tenant = get_tenant_from_request_or_default()
            if not tenant:
                return jsonify({"status": "error", "error": "Geen platformtenant geselecteerd."}), 400
            fallback_channel = requested_channel or ("email" if payload_email and not payload_phone else "sms")
            if fallback_channel == "email" and not payload_email:
                return jsonify({"status": "error", "error": "Geen e-mailadres gekoppeld aan dit gesprek."}), 400
            if fallback_channel == "sms" and not payload_phone:
                return jsonify({"status": "error", "error": "Geen telefoonnummer gekoppeld aan dit gesprek."}), 400
            conv = get_or_create_conversation(
                tenant,
                phone=payload_phone,
                name=payload_name,
                email=payload_email,
                channel=fallback_channel,
                subject=subject,
            )
            if not conv:
                return jsonify({"status": "error", "error": "Gesprek kon niet opnieuw worden aangemaakt."}), 500
            conversation_id = conv["id"]
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT tenant_id, contact_phone, contact_email, channel, subject FROM conversations WHERE id = %s LIMIT 1;",
                        (conversation_id,),
                    )
                    row = cur.fetchone()

        if not row:
            return jsonify({"status": "error", "error": "Gesprek niet gevonden."}), 404

        tenant_id, stored_phone, stored_email, stored_channel, stored_subject = row
        channel = requested_channel or stored_channel or "sms"
        phone = payload_phone or stored_phone or ""
        email_address = payload_email or stored_email or ""
        final_subject = subject or stored_subject or ""

        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE conversations
                    SET contact_name = COALESCE(NULLIF(%s, ''), contact_name),
                        contact_phone = COALESCE(NULLIF(%s, ''), contact_phone),
                        contact_email = COALESCE(NULLIF(%s, ''), contact_email),
                        channel = %s,
                        subject = COALESCE(NULLIF(%s, ''), subject),
                        updated_at = NOW()
                    WHERE id = %s;
                    """,
                    (payload_name, phone, email_address, channel, final_subject, conversation_id),
                )

        tenant = TENANTS_BY_ID.get(tenant_id)
        if not tenant:
            return jsonify({"status": "error", "error": "Tenant niet gevonden."}), 404

        if channel == "email":
            if not email_address:
                return jsonify({"status": "error", "error": "Geen e-mailadres gekoppeld aan dit gesprek."}), 400
            final_subject = final_subject or "Bericht van " + (tenant.get("company_name") or "Reactify")
            in_reply_to, references = _last_email_thread_headers(conversation_id)
            ok, external_id, send_error = send_email_message(
                tenant, email_address, final_subject, message,
                in_reply_to=in_reply_to, references=references,
            )
            if not ok:
                detail = send_error or "Onbekende SMTP-fout"
                return jsonify({
                    "status": "error",
                    "error": f"E-mail verzenden mislukt: {detail}",
                    "details": detail,
                }), 502
            msg_id = add_conversation_message(
                conversation_id, tenant_id, "outgoing", message, "email",
                external_id=external_id, sender_type="manual",
                subject=final_subject, in_reply_to=in_reply_to,
            )
        else:
            if not phone:
                return jsonify({"status": "error", "error": "Geen telefoonnummer gekoppeld aan dit gesprek."}), 400
            if not send_sms(tenant, normalize_phone(phone), message):
                return jsonify({"status": "error", "error": "SMSTools kon de SMS niet verzenden."}), 502
            msg_id = add_conversation_message(
                conversation_id, tenant_id, "outgoing", message, "sms", sender_type="manual"
            )
            channel = "sms"

        set_conversation_status(conversation_id, tenant_id, "manual_overname", True)
        return jsonify({
            "status": "success",
            "data": {
                "id": msg_id,
                "conversationId": conversation_id,
                "channel": channel,
                "subject": final_subject if channel == "email" else "",
                "status": "manual_overname",
                "aiEnabled": False,
            },
        }), 200
    except Exception as exc:
        log(f"❌ /send-message error: {exc}")
        return jsonify({
            "status": "error",
            "error": f"Bericht verzenden mislukt: {exc}",
            "details": str(exc),
        }), 500


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

