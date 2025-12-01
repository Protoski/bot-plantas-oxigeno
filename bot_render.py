"""
================================================================================
Bot de Telegram para Monitoreo de Plantas de Oxígeno PSA
VERSIÓN PARA RENDER.COM
================================================================================
"""

import os
import json
import logging
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path
from functools import wraps
from typing import Optional

from flask import Flask, request, jsonify
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# ================================================================================
# CONFIGURACIÓN DESDE VARIABLES DE ENTORNO
# ================================================================================

# Telegram (OBLIGATORIO - configurar en Render)
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")

# Admin principal (OBLIGATORIO - configurar en Render)
ADMIN_PRINCIPAL_ID = int(os.environ.get("ADMIN_PRINCIPAL_ID", "0"))

# API Key para ESP32 (OBLIGATORIO - configurar en Render)
API_KEY = os.environ.get("API_KEY", "clave_secreta_123")

# Puerto (Render lo asigna automáticamente)
PORT = int(os.environ.get("PORT", 5000))

# Base de datos (en Render usa /tmp para archivos temporales)
# Para producción real, usar PostgreSQL de Render
DATABASE_FILE = os.environ.get("DATABASE_PATH", "/tmp/plantas_oxigeno.db")
USUARIOS_FILE = os.environ.get("USUARIOS_PATH", "/tmp/usuarios_autorizados.json")

# Google Sheets (OPCIONAL)
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
SPREADSHEET_NAME = os.environ.get("SPREADSHEET_NAME", "Monitoreo_Plantas_Oxigeno")

# ================================================================================
# LOGGING
# ================================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ================================================================================
# FLASK APP
# ================================================================================

flask_app = Flask(__name__)

# ================================================================================
# BASE DE DATOS SQLITE
# ================================================================================

def inicializar_db():
    """Crea las tablas necesarias."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plantas (
            id TEXT PRIMARY KEY,
            nombre TEXT NOT NULL,
            ubicacion TEXT DEFAULT '',
            presion_bar REAL DEFAULT 0,
            temperatura_c REAL DEFAULT 0,
            pureza_pct REAL DEFAULT 0,
            flujo_nm3h REAL DEFAULT 0,
            horas_operacion INTEGER DEFAULT 0,
            modo TEXT DEFAULT 'Desconocido',
            alarma INTEGER DEFAULT 0,
            mensaje_alarma TEXT DEFAULT '',
            ultima_actualizacion TEXT,
            activa INTEGER DEFAULT 1
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historial (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            planta_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            presion_bar REAL,
            temperatura_c REAL,
            pureza_pct REAL,
            flujo_nm3h REAL,
            modo TEXT,
            alarma INTEGER,
            mensaje_alarma TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS config_alertas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            planta_id TEXT,
            intervalo_alerta_min INTEGER DEFAULT 5,
            ultima_alerta TEXT,
            alertas_activas INTEGER DEFAULT 1
        )
    """)
    
    conn.commit()
    conn.close()
    logger.info("Base de datos inicializada")


def obtener_plantas_db() -> dict:
    """Obtiene todas las plantas."""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM plantas WHERE activa = 1")
    rows = cursor.fetchall()
    conn.close()
    return {row["id"]: dict(row) for row in rows}


def actualizar_planta_db(planta_id: str, datos: dict):
    """Actualiza datos de una planta."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id FROM plantas WHERE id = ?", (planta_id,))
    existe = cursor.fetchone()
    
    timestamp = datetime.now().isoformat()
    
    if existe:
        cursor.execute("""
            UPDATE plantas SET
                nombre = COALESCE(?, nombre),
                presion_bar = ?,
                temperatura_c = ?,
                pureza_pct = ?,
                flujo_nm3h = ?,
                horas_operacion = COALESCE(?, horas_operacion),
                modo = ?,
                alarma = ?,
                mensaje_alarma = ?,
                ultima_actualizacion = ?
            WHERE id = ?
        """, (
            datos.get("nombre"),
            datos.get("presion_bar", 0),
            datos.get("temperatura_c", 0),
            datos.get("pureza_pct", 0),
            datos.get("flujo_nm3h", 0),
            datos.get("horas_operacion"),
            datos.get("modo", "Desconocido"),
            1 if datos.get("alarma") else 0,
            datos.get("mensaje_alarma", ""),
            timestamp,
            planta_id
        ))
    else:
        cursor.execute("""
            INSERT INTO plantas (id, nombre, presion_bar, temperatura_c, pureza_pct,
                                flujo_nm3h, horas_operacion, modo, alarma, 
                                mensaje_alarma, ultima_actualizacion, activa)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """, (
            planta_id,
            datos.get("nombre", f"Planta {planta_id}"),
            datos.get("presion_bar", 0),
            datos.get("temperatura_c", 0),
            datos.get("pureza_pct", 0),
            datos.get("flujo_nm3h", 0),
            datos.get("horas_operacion", 0),
            datos.get("modo", "Desconocido"),
            1 if datos.get("alarma") else 0,
            datos.get("mensaje_alarma", ""),
            timestamp
        ))
    
    # Historial
    cursor.execute("""
        INSERT INTO historial (planta_id, timestamp, presion_bar, temperatura_c,
                              pureza_pct, flujo_nm3h, modo, alarma, mensaje_alarma)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        planta_id, timestamp,
        datos.get("presion_bar", 0),
        datos.get("temperatura_c", 0),
        datos.get("pureza_pct", 0),
        datos.get("flujo_nm3h", 0),
        datos.get("modo", ""),
        1 if datos.get("alarma") else 0,
        datos.get("mensaje_alarma", "")
    ))
    
    conn.commit()
    conn.close()


def agregar_planta_db(planta_id: str, nombre: str, ubicacion: str = "") -> bool:
    """Agrega una nueva planta."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO plantas (id, nombre, ubicacion, ultima_actualizacion, activa)
            VALUES (?, ?, ?, ?, 1)
        """, (planta_id, nombre, ubicacion, datetime.now().isoformat()))
        
        cursor.execute("""
            INSERT INTO config_alertas (planta_id, intervalo_alerta_min, alertas_activas)
            VALUES (?, 5, 1)
        """, (planta_id,))
        
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False


def eliminar_planta_db(planta_id: str) -> bool:
    """Desactiva una planta."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("UPDATE plantas SET activa = 0 WHERE id = ?", (planta_id,))
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    return affected > 0


# ================================================================================
# GOOGLE SHEETS (OPCIONAL)
# ================================================================================

sheets_client = None

def inicializar_google_sheets():
    """Inicializa Google Sheets si hay credenciales."""
    global sheets_client
    
    if not GOOGLE_CREDENTIALS_JSON:
        logger.info("Google Sheets no configurado (sin credenciales)")
        return False
    
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        
        # Parsear credenciales desde variable de entorno
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        sheets_client = gspread.authorize(creds)
        
        logger.info("Google Sheets conectado")
        return True
        
    except Exception as e:
        logger.error(f"Error conectando Google Sheets: {e}")
        return False


def registrar_en_sheets(planta_id: str, datos: dict):
    """Registra datos en Google Sheets."""
    if not sheets_client:
        return
    
    try:
        spreadsheet = sheets_client.open(SPREADSHEET_NAME)
        
        # Hoja de historial
        try:
            ws = spreadsheet.worksheet("Historial")
        except:
            ws = spreadsheet.add_worksheet("Historial", rows=5000, cols=12)
            ws.append_row(["Timestamp", "Planta", "Presión", "Temp", "Pureza", "Flujo", "Modo", "Alarma"])
        
        ws.append_row([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            planta_id,
            datos.get("presion_bar", 0),
            datos.get("temperatura_c", 0),
            datos.get("pureza_pct", 0),
            datos.get("flujo_nm3h", 0),
            datos.get("modo", ""),
            "SÍ" if datos.get("alarma") else "No"
        ])
        
    except Exception as e:
        logger.error(f"Error registrando en Sheets: {e}")


# ================================================================================
# GESTIÓN DE USUARIOS
# ================================================================================

def cargar_usuarios() -> dict:
    """Carga usuarios autorizados."""
    try:
        if os.path.exists(USUARIOS_FILE):
            with open(USUARIOS_FILE, "r") as f:
                return json.load(f)
    except:
        pass
    
    return {
        "admins": [ADMIN_PRINCIPAL_ID] if ADMIN_PRINCIPAL_ID else [],
        "operadores": [],
        "lectores": [],
    }


def guardar_usuarios(usuarios: dict):
    """Guarda usuarios."""
    try:
        with open(USUARIOS_FILE, "w") as f:
            json.dump(usuarios, f, indent=2)
    except Exception as e:
        logger.error(f"Error guardando usuarios: {e}")


USUARIOS = cargar_usuarios()


def es_usuario_autorizado(user_id: int) -> bool:
    todos = USUARIOS.get("admins", []) + USUARIOS.get("operadores", []) + USUARIOS.get("lectores", [])
    return user_id in todos


def es_admin(user_id: int) -> bool:
    return user_id in USUARIOS.get("admins", [])


def es_operador_o_admin(user_id: int) -> bool:
    return user_id in USUARIOS.get("admins", []) + USUARIOS.get("operadores", [])


def obtener_rol(user_id: int) -> str:
    if user_id in USUARIOS.get("admins", []):
        return "admin"
    elif user_id in USUARIOS.get("operadores", []):
        return "operador"
    elif user_id in USUARIOS.get("lectores", []):
        return "lector"
    return "sin_acceso"


# ================================================================================
# DECORADORES
# ================================================================================

def requiere_autorizacion(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or not es_usuario_autorizado(user.id):
            texto = f"🚫 *Acceso Denegado*\n\nTu ID: `{user.id if user else 'N/A'}`"
            if update.message:
                await update.message.reply_text(texto, parse_mode="Markdown")
            elif update.callback_query:
                await update.callback_query.answer("⛔ Sin autorización", show_alert=True)
            return
        return await func(update, context)
    return wrapper


def requiere_admin(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or not es_admin(user.id):
            if update.message:
                await update.message.reply_text("🔐 Solo administradores.")
            return
        return await func(update, context)
    return wrapper


# ================================================================================
# SISTEMA DE ALERTAS
# ================================================================================

class SistemaAlertas:
    def __init__(self, bot_app):
        self.bot_app = bot_app
        self.ultima_alerta = {}
    
    def obtener_intervalo(self, planta_id: str) -> int:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT intervalo_alerta_min FROM config_alertas WHERE planta_id = ?", (planta_id,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else 5
    
    def puede_enviar(self, planta_id: str) -> bool:
        intervalo = self.obtener_intervalo(planta_id)
        if planta_id not in self.ultima_alerta:
            return True
        return datetime.now() - self.ultima_alerta[planta_id] >= timedelta(minutes=intervalo)
    
    async def enviar_alerta(self, planta_id: str, datos: dict):
        if not self.puede_enviar(planta_id):
            return
        
        usuarios = cargar_usuarios()
        todos = usuarios.get("admins", []) + usuarios.get("operadores", []) + usuarios.get("lectores", [])
        
        texto = (
            f"🚨 *ALERTA - {datos.get('nombre', planta_id)}*\n\n"
            f"⚠️ {datos.get('mensaje_alarma', 'Alarma activa')}\n\n"
            f"📊 Presión: {datos.get('presion_bar', 0):.1f} bar\n"
            f"🌡 Temp: {datos.get('temperatura_c', 0):.1f} °C\n"
            f"🧪 Pureza: {datos.get('pureza_pct', 0):.1f}%\n"
            f"💨 Flujo: {datos.get('flujo_nm3h', 0):.1f} Nm³/h"
        )
        
        for user_id in todos:
            try:
                await self.bot_app.bot.send_message(chat_id=user_id, text=texto, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Error enviando alerta a {user_id}: {e}")
        
        self.ultima_alerta[planta_id] = datetime.now()


sistema_alertas: Optional[SistemaAlertas] = None

# ================================================================================
# FUNCIONES DE FORMATO
# ================================================================================

def formatear_estado_planta(planta: dict) -> str:
    estado = "🟢" if planta.get("modo") == "Producción" and not planta.get("alarma") else \
             "🟡" if planta.get("modo") == "Mantenimiento" else "🔴"
    
    texto = [
        f"{estado} *{planta.get('nombre', 'Sin nombre')}*",
        f"📍 {planta.get('ubicacion', '')}",
        "",
        f"⚙️ Modo: *{planta.get('modo', '?')}*",
        "",
        f"🧪 Pureza: *{planta.get('pureza_pct', 0):.1f}%*",
        f"💨 Flujo: *{planta.get('flujo_nm3h', 0):.1f} Nm³/h*",
        f"📈 Presión: *{planta.get('presion_bar', 0):.1f} bar*",
        f"🌡 Temp: *{planta.get('temperatura_c', 0):.1f}°C*",
        "",
    ]
    
    if planta.get("alarma"):
        texto.append(f"🚨 *ALERTA:* {planta.get('mensaje_alarma', '')}")
    else:
        texto.append("✅ Sin alarmas")
    
    return "\n".join(texto)


# ================================================================================
# HANDLERS DE TELEGRAM
# ================================================================================

@requiere_autorizacion
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    plantas = obtener_plantas_db()
    
    keyboard = []
    fila = []
    
    for pid, p in plantas.items():
        estado = "🟢" if p.get("modo") == "Producción" and not p.get("alarma") else \
                 "🟡" if p.get("modo") == "Mantenimiento" else "🔴"
        fila.append(InlineKeyboardButton(f"{estado} {p.get('nombre', pid)[:12]}", callback_data=f"ver:{pid}"))
        if len(fila) == 2:
            keyboard.append(fila)
            fila = []
    if fila:
        keyboard.append(fila)
    
    keyboard.append([InlineKeyboardButton("📊 Resumen", callback_data="resumen:all")])
    
    if es_admin(user.id):
        keyboard.append([
            InlineKeyboardButton("➕ Nueva Planta", callback_data="admin:agregar"),
            InlineKeyboardButton("👥 Usuarios", callback_data="admin:usuarios"),
        ])
    
    texto = (
        f"👋 Hola *{user.first_name}*!\n\n"
        f"🔑 Rol: *{obtener_rol(user.id).upper()}*\n"
        f"📡 Plantas: *{len(plantas)}*\n\n"
        "Seleccioná una opción:"
    )
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.message:
        await update.message.reply_text(texto, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await update.callback_query.edit_message_text(texto, reply_markup=reply_markup, parse_mode="Markdown")


@requiere_autorizacion
async def manejar_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    data = query.data
    partes = data.split(":")
    
    if len(partes) < 2:
        return
    
    accion, parametro = partes[0], partes[1]
    plantas = obtener_plantas_db()
    
    if accion == "ver" and parametro in plantas:
        texto = formatear_estado_planta(plantas[parametro])
        keyboard = [[InlineKeyboardButton("🔁 Actualizar", callback_data=f"ver:{parametro}")]]
        if es_operador_o_admin(user.id):
            keyboard[0].append(InlineKeyboardButton("🔄 Cambiar modo", callback_data=f"modo:{parametro}"))
        keyboard.append([InlineKeyboardButton("⬅️ Menú", callback_data="menu:0")])
        await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "modo" and parametro in plantas:
        if es_operador_o_admin(user.id):
            nuevo = "Mantenimiento" if plantas[parametro].get("modo") == "Producción" else "Producción"
            actualizar_planta_db(parametro, {"modo": nuevo})
            plantas = obtener_plantas_db()
            texto = f"✅ Modo: {nuevo}\n\n" + formatear_estado_planta(plantas[parametro])
            keyboard = [
                [InlineKeyboardButton("🔁 Actualizar", callback_data=f"ver:{parametro}"),
                 InlineKeyboardButton("🔄 Cambiar modo", callback_data=f"modo:{parametro}")],
                [InlineKeyboardButton("⬅️ Menú", callback_data="menu:0")]
            ]
            await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "resumen":
        lineas = ["📊 *Resumen*\n"]
        for pid, p in plantas.items():
            estado = "🟢" if p.get("modo") == "Producción" and not p.get("alarma") else "🟡" if p.get("modo") == "Mantenimiento" else "🔴"
            lineas.append(f"{estado} {p.get('nombre', pid)}: {p.get('modo', '?')}")
        keyboard = [[InlineKeyboardButton("⬅️ Menú", callback_data="menu:0")]]
        await query.edit_message_text("\n".join(lineas), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "menu":
        await start(update, context)
    
    elif accion == "admin":
        if not es_admin(user.id):
            await query.answer("🔐 Solo admin", show_alert=True)
            return
        
        if parametro == "agregar":
            texto = "➕ *Nueva Planta*\n\nUsá:\n`/nueva_planta ID NOMBRE`\n\nEj: `/nueva_planta planta_3 Hospital Sur`"
        else:
            texto = "👥 *Usuarios*\n\nComandos:\n`/agregar_admin ID`\n`/agregar_operador ID`\n`/agregar_lector ID`\n`/remover_usuario ID`\n`/listar_usuarios`"
        
        keyboard = [[InlineKeyboardButton("⬅️ Volver", callback_data="menu:0")]]
        await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


# ================================================================================
# COMANDOS
# ================================================================================

@requiere_admin
async def nueva_planta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("⚠️ Uso: `/nueva_planta ID NOMBRE`", parse_mode="Markdown")
        return
    
    planta_id = context.args[0].lower().replace(" ", "_")
    nombre = " ".join(context.args[1:])
    
    if agregar_planta_db(planta_id, nombre):
        await update.message.reply_text(f"✅ Planta agregada:\n• ID: `{planta_id}`\n• Nombre: {nombre}", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"❌ Ya existe `{planta_id}`", parse_mode="Markdown")


@requiere_admin
async def eliminar_planta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ Uso: `/eliminar_planta ID`", parse_mode="Markdown")
        return
    
    if eliminar_planta_db(context.args[0]):
        await update.message.reply_text(f"✅ Planta eliminada", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"❌ No encontrada", parse_mode="Markdown")


@requiere_admin
async def agregar_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _agregar_usuario(update, context, "admins")

@requiere_admin
async def agregar_operador(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _agregar_usuario(update, context, "operadores")

@requiere_admin
async def agregar_lector(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _agregar_usuario(update, context, "lectores")


async def _agregar_usuario(update: Update, context: ContextTypes.DEFAULT_TYPE, rol: str):
    global USUARIOS
    if not context.args:
        await update.message.reply_text(f"⚠️ Uso: `/agregar_{rol[:-1]} ID`", parse_mode="Markdown")
        return
    
    try:
        nuevo_id = int(context.args[0])
    except:
        await update.message.reply_text("❌ ID debe ser número")
        return
    
    for r in ["admins", "operadores", "lectores"]:
        if nuevo_id in USUARIOS.get(r, []):
            await update.message.reply_text(f"⚠️ Ya tiene rol: {r}")
            return
    
    USUARIOS[rol].append(nuevo_id)
    guardar_usuarios(USUARIOS)
    await update.message.reply_text(f"✅ `{nuevo_id}` agregado como {rol[:-1]}", parse_mode="Markdown")


@requiere_admin
async def remover_usuario(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global USUARIOS
    if not context.args:
        await update.message.reply_text("⚠️ Uso: `/remover_usuario ID`", parse_mode="Markdown")
        return
    
    try:
        user_id = int(context.args[0])
    except:
        await update.message.reply_text("❌ ID debe ser número")
        return
    
    if user_id == ADMIN_PRINCIPAL_ID:
        await update.message.reply_text("❌ No se puede eliminar admin principal")
        return
    
    for rol in ["admins", "operadores", "lectores"]:
        if user_id in USUARIOS.get(rol, []):
            USUARIOS[rol].remove(user_id)
            guardar_usuarios(USUARIOS)
            await update.message.reply_text(f"✅ Usuario eliminado")
            return
    
    await update.message.reply_text(f"⚠️ No encontrado")


@requiere_admin
async def listar_usuarios(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = ["👥 *Usuarios*\n"]
    for rol, emoji in [("admins", "👑"), ("operadores", "🔧"), ("lectores", "👁")]:
        texto.append(f"*{emoji} {rol.title()}:*")
        for uid in USUARIOS.get(rol, []):
            texto.append(f"  `{uid}`")
        if not USUARIOS.get(rol):
            texto.append("  (ninguno)")
    await update.message.reply_text("\n".join(texto), parse_mode="Markdown")


async def mi_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(f"🆔 Tu ID: `{user.id}`", parse_mode="Markdown")


# ================================================================================
# API FLASK
# ================================================================================

@flask_app.route("/api/datos", methods=["POST"])
def recibir_datos():
    """Recibe datos del ESP32."""
    api_key = request.headers.get("X-API-Key")
    if api_key != API_KEY:
        return jsonify({"error": "No autorizado"}), 401
    
    try:
        datos = request.get_json()
        if not datos or "planta_id" not in datos:
            return jsonify({"error": "Datos incompletos"}), 400
        
        planta_id = datos["planta_id"]
        logger.info(f"Datos recibidos: {planta_id}")
        
        # Verificar alarma nueva
        plantas = obtener_plantas_db()
        alarma_anterior = plantas.get(planta_id, {}).get("alarma", False)
        
        # Guardar
        actualizar_planta_db(planta_id, datos)
        
        # Google Sheets
        registrar_en_sheets(planta_id, datos)
        
        # Alerta si es nueva
        if datos.get("alarma") and not alarma_anterior and sistema_alertas:
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(sistema_alertas.enviar_alerta(planta_id, datos))
        
        return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()}), 200
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route("/api/plantas", methods=["GET"])
def listar_plantas_api():
    api_key = request.headers.get("X-API-Key")
    if api_key != API_KEY:
        return jsonify({"error": "No autorizado"}), 401
    return jsonify(obtener_plantas_db()), 200


@flask_app.route("/api/planta/<planta_id>", methods=["GET"])
def obtener_planta_api(planta_id):
    api_key = request.headers.get("X-API-Key")
    if api_key != API_KEY:
        return jsonify({"error": "No autorizado"}), 401
    plantas = obtener_plantas_db()
    if planta_id in plantas:
        return jsonify(plantas[planta_id]), 200
    return jsonify({"error": "No encontrada"}), 404
@flask_app.route("/api/historial_json", methods=["GET"])
def historial_planta_json():
    """
    Devuelve el historial de una planta en formato JSON para usar en el dashboard.
    Parámetros:
      - api_key: seguridad básica por query string
      - planta_id: ID de la planta
      - desde (opcional): ISO 'YYYY-MM-DD' o 'YYYY-MM-DDTHH:MM'
      - hasta (opcional): mismo formato
    """
    api_key = request.args.get("api_key")
    if api_key != API_KEY:
        return jsonify({"error": "No autorizado"}), 401

    planta_id = request.args.get("planta_id")
    if not planta_id:
        return jsonify({"error": "Falta planta_id"}), 400

    desde = request.args.get("desde")
    hasta = request.args.get("hasta")

    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Construir query con filtros opcionales por fecha
    query = """
        SELECT planta_id, timestamp, presion_bar, temperatura_c,
               pureza_pct, flujo_nm3h, modo, alarma, mensaje_alarma
        FROM historial
        WHERE planta_id = ?
    """
    params = [planta_id]

    if desde:
        # Si solo viene fecha YYYY-MM-DD, completar con 00:00
        if len(desde) == 10:
            desde = desde + "T00:00:00"
        query += " AND timestamp >= ?"
        params.append(desde)

    if hasta:
        # Si solo viene fecha YYYY-MM-DD, completar con 23:59
        if len(hasta) == 10:
            hasta = hasta + "T23:59:59"
        query += " AND timestamp <= ?"
        params.append(hasta)

    query += " ORDER BY timestamp ASC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    datos = [dict(r) for r in rows]
    return jsonify(datos), 200


@flask_app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()}), 200


@flask_app.route("/", methods=["GET"])
def home():
    """Página principal."""
    plantas = obtener_plantas_db()
    return jsonify({
        "servicio": "Monitor Plantas O2 PSA",
        "estado": "activo",
        "plantas_registradas": len(plantas),
        "endpoints": {
            "POST /api/datos": "Recibir datos de ESP32",
            "GET /api/plantas": "Listar plantas",
            "GET /health": "Estado del servicio"
        }
    }), 200

@flask_app.route("/dashboard", methods=["GET"])
def dashboard():
    api_key = request.args.get("api_key")
    if api_key != API_KEY:
        return "No autorizado", 401

    plantas = obtener_plantas_db()

    # Si no hay plantas registradas todavía
    if not plantas:
        return """
        <html><body>
        <h2>No hay plantas registradas aún.</h2>
        <p>Esperá a que el ESP32 envíe datos o agrega una planta desde Telegram.</p>
        </body></html>
        """

    import json

    # Convertir dict de plantas a JSON para usar en JavaScript
    plantas_json = json.dumps(plantas)

    html = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <title>SCADA Plantas de Oxígeno</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {
                font-family: Arial, sans-serif;
                background: #0b1724;
                color: #ecf0f1;
                margin: 0;
                padding: 0;
            }
            header {
                background: #111827;
                padding: 15px 20px;
                display: flex;
                justify-content: space-between;
                align-items: center;
                box-shadow: 0 2px 5px rgba(0,0,0,0.5);
            }
            header h1 {
                margin: 0;
                font-size: 20px;
            }
            .container {
                display: grid;
                grid-template-columns: 260px 1fr;
                gap: 10px;
                padding: 10px;
            }
            .sidebar {
                background: #111827;
                border-radius: 8px;
                padding: 10px;
            }
            .sidebar h2 {
                font-size: 16px;
                margin-top: 0;
            }
            .plant-list {
                list-style: none;
                margin: 0;
                padding: 0;
                max-height: 400px;
                overflow-y: auto;
            }
            .plant-item {
                padding: 8px;
                margin-bottom: 5px;
                border-radius: 6px;
                cursor: pointer;
                display: flex;
                justify-content: space-between;
                align-items: center;
                font-size: 13px;
            }
            .plant-item span.nombre {
                flex: 1;
            }
            .plant-item:hover {
                background: #1f2937;
            }
            .plant-item.selected {
                background: #2563eb;
            }
            .estado-dot {
                width: 10px;
                height: 10px;
                border-radius: 50%;
                margin-left: 8px;
            }
            .dot-ok { background: #22c55e; }
            .dot-mantenimiento { background: #eab308; }
            .dot-alarma { background: #ef4444; }

            .content {
                display: flex;
                flex-direction: column;
                gap: 10px;
            }
            .cards-row {
                display: grid;
                grid-template-columns: repeat(4, 1fr);
                gap: 10px;
            }
            .card {
                background: #111827;
                border-radius: 8px;
                padding: 10px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.5);
            }
            .card h3 {
                margin: 0 0 5px 0;
                font-size: 13px;
                color: #9ca3af;
            }
            .card .valor {
                font-size: 22px;
                font-weight: bold;
            }
            .card .unidad {
                font-size: 13px;
                color: #9ca3af;
                margin-left: 5px;
            }
            .card .subtexto {
                font-size: 11px;
                color: #9ca3af;
                margin-top: 3px;
            }
            .card-alerta {
                border-left: 4px solid #ef4444;
            }
            .filtros {
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
                align-items: center;
                font-size: 13px;
            }
            .filtros input {
                background: #020617;
                border: 1px solid #374151;
                border-radius: 4px;
                padding: 4px 6px;
                color: #e5e7eb;
            }
            .filtros button {
                background: #2563eb;
                border: none;
                border-radius: 4px;
                padding: 5px 10px;
                color: white;
                cursor: pointer;
                font-size: 13px;
            }
            .filtros button:hover {
                background: #1d4ed8;
            }
            .charts-grid {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 10px;
            }
            .chart-card {
                background: #111827;
                border-radius: 8px;
                padding: 10px;
            }
            .chart-card canvas {
                max-height: 260px;
            }
            .footer {
                text-align: center;
                font-size: 11px;
                color: #6b7280;
                padding: 8px;
            }
            @media (max-width: 900px) {
                .container {
                    grid-template-columns: 1fr;
                }
                .cards-row {
                    grid-template-columns: repeat(2, 1fr);
                }
                .charts-grid {
                    grid-template-columns: 1fr;
                }
            }
        </style>
    </head>
    <body>
        <header>
            <h1>SCADA - Monitor de Plantas de Oxígeno</h1>
            <div style="font-size:13px;color:#9ca3af;">MSPyBS-DGGIES</div>
        </header>
        <div class="container">
            <div class="sidebar">
                <h2>Plantas</h2>
                <ul class="plant-list" id="plantList"></ul>
            </div>
            <div class="content">
                <div class="filtros">
                    <div>
                        Planta seleccionada: <strong id="plantaSeleccionadaLabel"></strong>
                    </div>
                    <div>
                        Desde:
                        <input type="datetime-local" id="filtroDesde">
                    </div>
                    <div>
                        Hasta:
                        <input type="datetime-local" id="filtroHasta">
                    </div>
                    <button id="btnAplicarFiltros">Aplicar filtros</button>
                    <button id="btnUltimas24">Últimas 24h</button>
                </div>

                <div class="cards-row">
                    <div class="card">
                        <h3>Pureza O₂</h3>
                        <div class="valor" id="cardPureza">--</div>
                        <div class="subtexto" id="cardPurezaEstado"></div>
                    </div>
                    <div class="card">
                        <h3>Flujo</h3>
                        <div class="valor" id="cardFlujo">--</div>
                        <div class="subtexto">Nm³/h</div>
                    </div>
                    <div class="card">
                        <h3>Presión</h3>
                        <div class="valor" id="cardPresion">--</div>
                        <div class="subtexto">bar</div>
                    </div>
                    <div class="card">
                        <h3>Temperatura</h3>
                        <div class="valor" id="cardTemp">--</div>
                        <div class="subtexto">°C</div>
                    </div>
                </div>

                <div class="card card-alerta" id="cardAlarma" style="display:none;">
                    <h3>Alarma Activa</h3>
                    <div class="valor" id="textoAlarma">🚨</div>
                    <div class="subtexto" id="detalleAlarma"></div>
                </div>

                <div class="charts-grid">
                    <div class="chart-card">
                        <h3>Pureza O₂ (%)</h3>
                        <canvas id="chartPureza"></canvas>
                    </div>
                    <div class="chart-card">
                        <h3>Flujo (Nm³/h)</h3>
                        <canvas id="chartFlujo"></canvas>
                    </div>
                    <div class="chart-card">
                        <h3>Presión (bar)</h3>
                        <canvas id="chartPresion"></canvas>
                    </div>
                    <div class="chart-card">
                        <h3>Temperatura (°C)</h3>
                        <canvas id="chartTemp"></canvas>
                    </div>
                </div>
            </div>
        </div>
        <div class="footer">
            SCADA simplificado para monitoreo remoto de plantas PSA de oxígeno - MSPyBS - DGGIES
        </div>

        <script>
            const PLANTAS = """ + plantas_json + """;
            const API_KEY = '""" + API_KEY + """';

            let plantaSeleccionada = null;
            let chartPureza = null;
            let chartFlujo = null;
            let chartPresion = null;
            let chartTemp = null;

            function estadoPlanta(planta) {
                const modo = planta.modo || '';
                const alarma = planta.alarma;
                if (alarma) return 'alarma';
                if (modo === 'Mantenimiento') return 'mantenimiento';
                return 'ok';
            }

            function crearListaPlantas() {
                const lista = document.getElementById('plantList');
                lista.innerHTML = '';

                const ids = Object.keys(PLANTAS);
                if (!plantaSeleccionada && ids.length > 0) {
                    plantaSeleccionada = ids[0];
                }

                ids.forEach(id => {
                    const p = PLANTAS[id];
                    const li = document.createElement('li');
                    li.className = 'plant-item' + (id === plantaSeleccionada ? ' selected' : '');
                    li.dataset.id = id;

                    const nombre = document.createElement('span');
                    nombre.className = 'nombre';
                    nombre.textContent = p.nombre || id;

                    const dot = document.createElement('div');
                    dot.className = 'estado-dot';
                    const estado = estadoPlanta(p);
                    if (estado === 'ok') dot.classList.add('dot-ok');
                    if (estado === 'mantenimiento') dot.classList.add('dot-mantenimiento');
                    if (estado === 'alarma') dot.classList.add('dot-alarma');

                    li.appendChild(nombre);
                    li.appendChild(dot);

                    li.addEventListener('click', () => {
                        plantaSeleccionada = id;
                        crearListaPlantas();
                        cargarDatos();
                    });

                    lista.appendChild(li);
                });

                const label = document.getElementById('plantaSeleccionadaLabel');
                if (plantaSeleccionada && PLANTAS[plantaSeleccionada]) {
                    label.textContent = PLANTAS[plantaSeleccionada].nombre || plantaSeleccionada;
                } else {
                    label.textContent = '(ninguna)';
                }
            }

            function formatearFechaLocal(iso) {
                if (!iso) return '';
                const d = new Date(iso);
                if (isNaN(d.getTime())) return iso;
                return d.toLocaleString();
            }

            function actualizarTarjetas(ultimo) {
                const cardPureza = document.getElementById('cardPureza');
                const cardPurezaEstado = document.getElementById('cardPurezaEstado');
                const cardFlujo = document.getElementById('cardFlujo');
                const cardPresion = document.getElementById('cardPresion');
                const cardTemp = document.getElementById('cardTemp');
                const cardAlarma = document.getElementById('cardAlarma');
                const textoAlarma = document.getElementById('textoAlarma');
                const detalleAlarma = document.getElementById('detalleAlarma');

                if (!ultimo) {
                    cardPureza.textContent = '--';
                    cardPurezaEstado.textContent = '';
                    cardFlujo.textContent = '--';
                    cardPresion.textContent = '--';
                    cardTemp.textContent = '--';
                    cardAlarma.style.display = 'none';
                    return;
                }

                cardPureza.textContent = (ultimo.pureza_pct ?? 0).toFixed(1) + ' %';
                cardFlujo.textContent = (ultimo.flujo_nm3h ?? 0).toFixed(1);
                cardPresion.textContent = (ultimo.presion_bar ?? 0).toFixed(1);
                cardTemp.textContent = (ultimo.temperatura_c ?? 0).toFixed(1);

                const pureza = ultimo.pureza_pct ?? 0;
                if (pureza >= 93) {
                    cardPurezaEstado.textContent = 'Dentro de rango para uso médico (≥93%)';
                    cardPurezaEstado.style.color = '#22c55e';
                } else {
                    cardPurezaEstado.textContent = 'Por debajo del rango recomendado (<93%)';
                    cardPurezaEstado.style.color = '#ef4444';
                }

                if (ultimo.alarma) {
                    cardAlarma.style.display = 'block';
                    textoAlarma.textContent = '🚨 ALARMA';
                    detalleAlarma.textContent = (ultimo.mensaje_alarma || '') + ' | ' + formatearFechaLocal(ultimo.timestamp);
                } else {
                    cardAlarma.style.display = 'none';
                }
            }

            function crearOActualizarChart(chartRef, ctx, label, labels, data) {
                if (chartRef) {
                    chartRef.data.labels = labels;
                    chartRef.data.datasets[0].data = data;
                    chartRef.update();
                    return chartRef;
                } else {
                    return new Chart(ctx, {
                        type: 'line',
                        data: {
                            labels: labels,
                            datasets: [{
                                label: label,
                                data: data,
                                borderWidth: 1.5,
                                tension: 0.2,
                                pointRadius: 0
                            }]
                        },
                        options: {
                            responsive: true,
                            plugins: {
                                legend: {
                                    labels: {
                                        color: '#e5e7eb'
                                    }
                                }
                            },
                            scales: {
                                x: {
                                    ticks: {
                                        color: '#9ca3af',
                                        maxTicksLimit: 6
                                    }
                                },
                                y: {
                                    ticks: {
                                        color: '#9ca3af'
                                    }
                                }
                            }
                        }
                    });
                }
            }

            function cargarDatos() {
                if (!plantaSeleccionada) return;

                const desdeInput = document.getElementById('filtroDesde').value;
                const hastaInput = document.getElementById('filtroHasta').value;

                let url = '/api/historial_json?api_key=' + encodeURIComponent(API_KEY) +
                          '&planta_id=' + encodeURIComponent(plantaSeleccionada);

                if (desdeInput) {
                    url += '&desde=' + encodeURIComponent(desdeInput);
                }
                if (hastaInput) {
                    url += '&hasta=' + encodeURIComponent(hastaInput);
                }

                fetch(url)
                    .then(r => r.json())
                    .then(datos => {
                        if (!Array.isArray(datos) || datos.length === 0) {
                            actualizarTarjetas(null);
                            const ctxP = document.getElementById('chartPureza').getContext('2d');
                            const ctxF = document.getElementById('chartFlujo').getContext('2d');
                            const ctxPr = document.getElementById('chartPresion').getContext('2d');
                            const ctxT = document.getElementById('chartTemp').getContext('2d');
                            const labels = [];
                            const vacio = [];

                            chartPureza = crearOActualizarChart(chartPureza, ctxP, 'Pureza (%)', labels, vacio);
                            chartFlujo = crearOActualizarChart(chartFlujo, ctxF, 'Flujo (Nm³/h)', labels, vacio);
                            chartPresion = crearOActualizarChart(chartPresion, ctxPr, 'Presión (bar)', labels, vacio);
                            chartTemp = crearOActualizarChart(chartTemp, ctxT, 'Temperatura (°C)', labels, vacio);
                            return;
                        }

                        const labels = datos.map(d => {
                            const dt = new Date(d.timestamp);
                            return dt.toLocaleTimeString();
                        });

                        const pureza = datos.map(d => d.pureza_pct ?? 0);
                        const flujo = datos.map(d => d.flujo_nm3h ?? 0);
                        const presion = datos.map(d => d.presion_bar ?? 0);
                        const temp = datos.map(d => d.temperatura_c ?? 0);

                        const ultimo = datos[datos.length - 1];
                        actualizarTarjetas(ultimo);

                        const ctxP = document.getElementById('chartPureza').getContext('2d');
                        const ctxF = document.getElementById('chartFlujo').getContext('2d');
                        const ctxPr = document.getElementById('chartPresion').getContext('2d');
                        const ctxT = document.getElementById('chartTemp').getContext('2d');

                        chartPureza = crearOActualizarChart(chartPureza, ctxP, 'Pureza (%)', labels, pureza);
                        chartFlujo = crearOActualizarChart(chartFlujo, ctxF, 'Flujo (Nm³/h)', labels, flujo);
                        chartPresion = crearOActualizarChart(chartPresion, ctxPr, 'Presión (bar)', labels, presion);
                        chartTemp = crearOActualizarChart(chartTemp, ctxT, 'Temperatura (°C)', labels, temp);
                    })
                    .catch(err => {
                        console.error('Error cargando datos', err);
                    });
            }

            document.addEventListener('DOMContentLoaded', () => {
                crearListaPlantas();
                cargarDatos();

                document.getElementById('btnAplicarFiltros').addEventListener('click', () => {
                    cargarDatos();
                });

                document.getElementById('btnUltimas24').addEventListener('click', () => {
                    const ahora = new Date();
                    const hace24 = new Date(ahora.getTime() - 24 * 60 * 60 * 1000);

                    const formato = d => {
                        const pad = n => n.toString().padStart(2, '0');
                        return d.getFullYear() + '-' +
                               pad(d.getMonth() + 1) + '-' +
                               pad(d.getDate()) + 'T' +
                               pad(d.getHours()) + ':' +
                               pad(d.getMinutes());
                    };

                    document.getElementById('filtroDesde').value = formato(hace24);
                    document.getElementById('filtroHasta').value = formato(ahora);
                    cargarDatos();
                });
            });
        </script>
    </body>
    </html>
    """

    return html

# ================================================================================
# MAIN
# ================================================================================

def run_flask():
    """Ejecuta Flask."""
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)


def main():
    global sistema_alertas, USUARIOS
    
    print("=" * 50)
    print("  BOT PLANTAS O2 - RENDER.COM")
    print("=" * 50)
    
    # Verificar configuración
    if not TELEGRAM_TOKEN:
        print("❌ ERROR: Falta TELEGRAM_TOKEN")
        print("   Configuralo en las variables de entorno de Render")
        return
    
    if not ADMIN_PRINCIPAL_ID:
        print("❌ ERROR: Falta ADMIN_PRINCIPAL_ID")
        return
    
    print(f"✓ Token configurado")
    print(f"✓ Admin: {ADMIN_PRINCIPAL_ID}")
    print(f"✓ Puerto: {PORT}")
    
    # Inicializar DB
    inicializar_db()
    
    # Cargar usuarios
    USUARIOS = cargar_usuarios()
    
    # Google Sheets
    inicializar_google_sheets()
    
    # Telegram App
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Sistema de alertas
    sistema_alertas = SistemaAlertas(app)
    
    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("mi_id", mi_id))
    app.add_handler(CommandHandler("nueva_planta", nueva_planta))
    app.add_handler(CommandHandler("eliminar_planta", eliminar_planta))
    app.add_handler(CommandHandler("agregar_admin", agregar_admin))
    app.add_handler(CommandHandler("agregar_operador", agregar_operador))
    app.add_handler(CommandHandler("agregar_lector", agregar_lector))
    app.add_handler(CommandHandler("remover_usuario", remover_usuario))
    app.add_handler(CommandHandler("listar_usuarios", listar_usuarios))
    app.add_handler(CallbackQueryHandler(manejar_callback))
    
    # Iniciar Flask en hilo separado
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print(f"✓ API iniciada en puerto {PORT}")
    
    print("=" * 50)
    print("Bot iniciado!")
    print("=" * 50)
    
    # Iniciar bot
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
