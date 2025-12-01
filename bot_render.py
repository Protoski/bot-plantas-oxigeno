"""
================================================================================
Bot de Telegram para Monitoreo de Plantas de Oxígeno PSA
VERSIÓN CON POSTGRESQL PARA RENDER (DATOS PERSISTENTES)
================================================================================
"""

import os
import json
import logging
import threading
import csv
import io
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, List, Dict
from statistics import mean, stdev, median
from contextlib import contextmanager

from flask import Flask, request, jsonify, Response
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# PostgreSQL
import psycopg2
from psycopg2.extras import RealDictCursor

# ================================================================================
# CONFIGURACIÓN
# ================================================================================

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
ADMIN_PRINCIPAL_ID = int(os.environ.get("ADMIN_PRINCIPAL_ID", "0"))
API_KEY = os.environ.get("API_KEY", "clave_secreta_123")
PORT = int(os.environ.get("PORT", 5000))

# PostgreSQL - Render provee DATABASE_URL automáticamente
DATABASE_URL = os.environ.get("DATABASE_URL", "")

USUARIOS_FILE = os.environ.get("USUARIOS_PATH", "/tmp/usuarios_autorizados.json")

# Google Sheets (OPCIONAL)
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
SPREADSHEET_NAME = os.environ.get("SPREADSHEET_NAME", "Monitoreo_Plantas_Oxigeno")

# Umbrales
PUREZA_MINIMA = float(os.environ.get("PUREZA_MINIMA", "93.0"))
PRESION_MAXIMA = float(os.environ.get("PRESION_MAXIMA", "7.0"))
TEMPERATURA_MAXIMA = float(os.environ.get("TEMPERATURA_MAXIMA", "45.0"))

# ================================================================================
# LOGGING
# ================================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ================================================================================
# FLASK APP
# ================================================================================

flask_app = Flask(__name__)

# ================================================================================
# CONEXIÓN POSTGRESQL
# ================================================================================

def get_db_connection():
    if not DATABASE_URL:
        raise Exception("DATABASE_URL no configurada")
    
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    
    return psycopg2.connect(url, cursor_factory=RealDictCursor)


@contextmanager
def get_db():
    conn = get_db_connection()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def inicializar_db():
    with get_db() as conn:
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
                ultima_actualizacion TIMESTAMP,
                activa INTEGER DEFAULT 1
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS historial (
                id SERIAL PRIMARY KEY,
                planta_id TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                presion_bar REAL,
                temperatura_c REAL,
                pureza_pct REAL,
                flujo_nm3h REAL,
                modo TEXT,
                alarma INTEGER,
                mensaje_alarma TEXT,
                horas_operacion INTEGER DEFAULT 0
            )
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_hist_planta ON historial(planta_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_hist_ts ON historial(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_hist_planta_ts ON historial(planta_id, timestamp)")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS config_alertas (
                id SERIAL PRIMARY KEY,
                planta_id TEXT UNIQUE,
                intervalo_alerta_min INTEGER DEFAULT 5,
                alertas_activas INTEGER DEFAULT 1,
                pureza_minima REAL DEFAULT 93.0,
                presion_maxima REAL DEFAULT 7.0,
                temperatura_maxima REAL DEFAULT 45.0
            )
        """)
        
        logger.info("PostgreSQL inicializado")


def obtener_plantas_db() -> dict:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM plantas WHERE activa = 1")
        rows = cursor.fetchall()
        return {row["id"]: dict(row) for row in rows}


def actualizar_planta_db(planta_id: str, datos: dict):
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM plantas WHERE id = %s", (planta_id,))
        existe = cursor.fetchone()
        
        timestamp = datetime.now()
        
        if existe:
            cursor.execute("""
                UPDATE plantas SET
                    nombre = COALESCE(%s, nombre),
                    presion_bar = %s,
                    temperatura_c = %s,
                    pureza_pct = %s,
                    flujo_nm3h = %s,
                    horas_operacion = COALESCE(%s, horas_operacion),
                    modo = %s,
                    alarma = %s,
                    mensaje_alarma = %s,
                    ultima_actualizacion = %s
                WHERE id = %s
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
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
        
        cursor.execute("""
            INSERT INTO historial (planta_id, timestamp, presion_bar, temperatura_c,
                                  pureza_pct, flujo_nm3h, modo, alarma, mensaje_alarma, horas_operacion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            planta_id, timestamp,
            datos.get("presion_bar", 0),
            datos.get("temperatura_c", 0),
            datos.get("pureza_pct", 0),
            datos.get("flujo_nm3h", 0),
            datos.get("modo", ""),
            1 if datos.get("alarma") else 0,
            datos.get("mensaje_alarma", ""),
            datos.get("horas_operacion", 0)
        ))


def agregar_planta_db(planta_id: str, nombre: str, ubicacion: str = "") -> bool:
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO plantas (id, nombre, ubicacion, ultima_actualizacion, activa)
                VALUES (%s, %s, %s, %s, 1)
            """, (planta_id, nombre, ubicacion, datetime.now()))
            
            cursor.execute("""
                INSERT INTO config_alertas (planta_id, intervalo_alerta_min, alertas_activas)
                VALUES (%s, 5, 1) ON CONFLICT (planta_id) DO NOTHING
            """, (planta_id,))
            return True
    except psycopg2.IntegrityError:
        return False


def eliminar_planta_db(planta_id: str) -> bool:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE plantas SET activa = 0 WHERE id = %s", (planta_id,))
        return cursor.rowcount > 0


def obtener_historial_db(planta_id: str, desde: str = None, hasta: str = None, limite: int = None) -> List[Dict]:
    with get_db() as conn:
        cursor = conn.cursor()
        
        query = """
            SELECT planta_id, timestamp, presion_bar, temperatura_c,
                   pureza_pct, flujo_nm3h, modo, alarma, mensaje_alarma, horas_operacion
            FROM historial WHERE planta_id = %s
        """
        params = [planta_id]
        
        if desde:
            if len(desde) == 10:
                desde = desde + "T00:00:00"
            query += " AND timestamp >= %s"
            params.append(desde)
        
        if hasta:
            if len(hasta) == 10:
                hasta = hasta + "T23:59:59"
            query += " AND timestamp <= %s"
            params.append(hasta)
        
        query += " ORDER BY timestamp ASC"
        
        if limite:
            query += f" LIMIT {limite}"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        result = []
        for row in rows:
            r = dict(row)
            if r.get('timestamp'):
                r['timestamp'] = r['timestamp'].isoformat()
            result.append(r)
        
        return result


def calcular_estadisticas(datos: List[Dict]) -> Dict:
    if not datos:
        return {}
    
    def safe_stats(values):
        values = [v for v in values if v is not None]
        if not values:
            return {"min": 0, "max": 0, "avg": 0, "std": 0, "count": 0}
        return {
            "min": round(min(values), 2),
            "max": round(max(values), 2),
            "avg": round(mean(values), 2),
            "std": round(stdev(values), 2) if len(values) > 1 else 0,
            "count": len(values)
        }
    
    pureza = [d.get("pureza_pct", 0) for d in datos]
    flujo = [d.get("flujo_nm3h", 0) for d in datos]
    presion = [d.get("presion_bar", 0) for d in datos]
    temperatura = [d.get("temperatura_c", 0) for d in datos]
    
    alarmas = sum(1 for d in datos if d.get("alarma"))
    
    modos = {}
    for d in datos:
        modo = d.get("modo", "Desconocido")
        modos[modo] = modos.get(modo, 0) + 1
    
    tiempo_produccion = modos.get("Producción", 0)
    disponibilidad = (tiempo_produccion / len(datos) * 100) if datos else 0
    pureza_ok = sum(1 for p in pureza if p >= 93)
    cumplimiento_pureza = (pureza_ok / len(pureza) * 100) if pureza else 0
    
    return {
        "periodo": {"registros": len(datos)},
        "pureza": safe_stats(pureza),
        "flujo": safe_stats(flujo),
        "presion": safe_stats(presion),
        "temperatura": safe_stats(temperatura),
        "alarmas": {"total": alarmas},
        "kpis": {
            "disponibilidad": round(disponibilidad, 2),
            "cumplimiento_pureza": round(cumplimiento_pureza, 2)
        }
    }


def obtener_estadisticas_globales() -> Dict:
    plantas = obtener_plantas_db()
    
    total = len(plantas)
    operando = sum(1 for p in plantas.values() if p.get("modo") == "Producción")
    mant = sum(1 for p in plantas.values() if p.get("modo") == "Mantenimiento")
    alarma = sum(1 for p in plantas.values() if p.get("alarma"))
    
    if plantas:
        pureza_prom = mean([p.get("pureza_pct", 0) or 0 for p in plantas.values()])
        flujo_total = sum([p.get("flujo_nm3h", 0) or 0 for p in plantas.values()])
    else:
        pureza_prom = 0
        flujo_total = 0
    
    return {
        "total_plantas": total,
        "plantas_operando": operando,
        "plantas_mantenimiento": mant,
        "plantas_alarma": alarma,
        "pureza_promedio": round(pureza_prom, 2),
        "flujo_total": round(flujo_total, 2)
    }


# ================================================================================
# GESTIÓN DE USUARIOS
# ================================================================================

def cargar_usuarios() -> dict:
    try:
        if os.path.exists(USUARIOS_FILE):
            with open(USUARIOS_FILE, "r") as f:
                return json.load(f)
    except:
        pass
    return {"admins": [ADMIN_PRINCIPAL_ID] if ADMIN_PRINCIPAL_ID else [], "operadores": [], "lectores": []}


def guardar_usuarios(usuarios: dict):
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
    
    async def enviar_alerta(self, planta_id: str, datos: dict):
        if planta_id in self.ultima_alerta:
            if datetime.now() - self.ultima_alerta[planta_id] < timedelta(minutes=5):
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
                logger.error(f"Error alerta: {e}")
        
        self.ultima_alerta[planta_id] = datetime.now()


sistema_alertas: Optional[SistemaAlertas] = None


# ================================================================================
# FUNCIONES DE FORMATO
# ================================================================================

def formatear_estado_planta(planta: dict) -> str:
    estado = "🟢" if planta.get("modo") == "Producción" and not planta.get("alarma") else \
             "🟡" if planta.get("modo") == "Mantenimiento" else "🔴"
    
    pureza = planta.get("pureza_pct", 0) or 0
    pureza_icon = "✅" if pureza >= 93 else "⚠️"
    
    texto = [
        f"{estado} *{planta.get('nombre', 'Sin nombre')}*",
        f"📍 {planta.get('ubicacion', '')}",
        "",
        f"⚙️ Modo: *{planta.get('modo', '?')}*",
        f"🕐 Horas: *{planta.get('horas_operacion', 0):,}h*",
        "",
        f"🧪 Pureza: *{pureza:.1f}%* {pureza_icon}",
        f"💨 Flujo: *{planta.get('flujo_nm3h', 0) or 0:.1f} Nm³/h*",
        f"📈 Presión: *{planta.get('presion_bar', 0) or 0:.1f} bar*",
        f"🌡 Temp: *{planta.get('temperatura_c', 0) or 0:.1f}°C*",
    ]
    
    if planta.get("alarma"):
        texto.append(f"\n🚨 *ALERTA:* {planta.get('mensaje_alarma', '')}")
    else:
        texto.append("\n✅ Sin alarmas")
    
    return "\n".join(texto)


# ================================================================================
# HANDLERS DE TELEGRAM
# ================================================================================

@requiere_autorizacion
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    plantas = obtener_plantas_db()
    stats = obtener_estadisticas_globales()
    
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
    
    keyboard.append([
        InlineKeyboardButton("📊 Resumen", callback_data="resumen:all"),
        InlineKeyboardButton("📈 Stats", callback_data="stats:global")
    ])
    
    if es_admin(user.id):
        keyboard.append([
            InlineKeyboardButton("➕ Nueva Planta", callback_data="admin:agregar"),
            InlineKeyboardButton("👥 Usuarios", callback_data="admin:usuarios"),
        ])
    
    texto = (
        f"👋 Hola *{user.first_name}*!\n\n"
        f"🔑 Rol: *{obtener_rol(user.id).upper()}*\n\n"
        f"📡 *Estado:*\n"
        f"├ Total: *{stats['total_plantas']}* plantas\n"
        f"├ Operando: *{stats['plantas_operando']}* 🟢\n"
        f"├ Mant: *{stats['plantas_mantenimiento']}* 🟡\n"
        f"├ Alarma: *{stats['plantas_alarma']}* 🔴\n"
        f"├ Pureza: *{stats['pureza_promedio']:.1f}%*\n"
        f"└ Flujo: *{stats['flujo_total']:.1f} Nm³/h*"
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
        keyboard = [
            [InlineKeyboardButton("🔁 Actualizar", callback_data=f"ver:{parametro}"),
             InlineKeyboardButton("📈 Stats 24h", callback_data=f"stats24:{parametro}")],
        ]
        if es_operador_o_admin(user.id):
            keyboard.append([InlineKeyboardButton("🔄 Cambiar modo", callback_data=f"modo:{parametro}")])
        keyboard.append([InlineKeyboardButton("⬅️ Menú", callback_data="menu:0")])
        await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "modo" and parametro in plantas:
        if es_operador_o_admin(user.id):
            nuevo = "Mantenimiento" if plantas[parametro].get("modo") == "Producción" else "Producción"
            actualizar_planta_db(parametro, {"modo": nuevo})
            plantas = obtener_plantas_db()
            texto = f"✅ Modo: *{nuevo}*\n\n" + formatear_estado_planta(plantas[parametro])
            keyboard = [[InlineKeyboardButton("⬅️ Menú", callback_data="menu:0")]]
            await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "stats24" and parametro in plantas:
        desde = (datetime.now() - timedelta(hours=24)).isoformat()
        datos = obtener_historial_db(parametro, desde=desde)
        stats = calcular_estadisticas(datos)
        
        if stats:
            texto = (
                f"📊 *Stats 24h - {plantas[parametro].get('nombre')}*\n\n"
                f"📝 Registros: {stats['periodo']['registros']}\n\n"
                f"🧪 Pureza: {stats['pureza']['min']:.1f} - {stats['pureza']['max']:.1f}% (prom: {stats['pureza']['avg']:.1f}%)\n"
                f"💨 Flujo: {stats['flujo']['avg']:.1f} Nm³/h promedio\n\n"
                f"🎯 Disponibilidad: {stats['kpis']['disponibilidad']:.1f}%\n"
                f"✅ Cumpl. Pureza: {stats['kpis']['cumplimiento_pureza']:.1f}%"
            )
        else:
            texto = "📊 Sin datos en las últimas 24h"
        
        keyboard = [[InlineKeyboardButton("⬅️ Volver", callback_data=f"ver:{parametro}")]]
        await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "stats" and parametro == "global":
        stats = obtener_estadisticas_globales()
        texto = (
            f"📊 *Estadísticas Globales*\n\n"
            f"📡 Total: *{stats['total_plantas']}*\n"
            f"🟢 Operando: *{stats['plantas_operando']}*\n"
            f"🟡 Mant: *{stats['plantas_mantenimiento']}*\n"
            f"🔴 Alarma: *{stats['plantas_alarma']}*\n\n"
            f"🧪 Pureza prom: *{stats['pureza_promedio']:.1f}%*\n"
            f"💨 Flujo total: *{stats['flujo_total']:.1f} Nm³/h*"
        )
        keyboard = [[InlineKeyboardButton("⬅️ Menú", callback_data="menu:0")]]
        await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "resumen":
        lineas = ["📊 *Resumen*\n"]
        for pid, p in plantas.items():
            estado = "🟢" if p.get("modo") == "Producción" and not p.get("alarma") else "🟡" if p.get("modo") == "Mantenimiento" else "🔴"
            pur = p.get("pureza_pct", 0) or 0
            flu = p.get("flujo_nm3h", 0) or 0
            lineas.append(f"{estado} *{p.get('nombre', pid)}*: {pur:.1f}% | {flu:.1f} Nm³/h")
        keyboard = [[InlineKeyboardButton("⬅️ Menú", callback_data="menu:0")]]
        await query.edit_message_text("\n".join(lineas), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "menu":
        await start(update, context)
    
    elif accion == "admin":
        if not es_admin(user.id):
            await query.answer("🔐 Solo admin", show_alert=True)
            return
        
        if parametro == "agregar":
            texto = "➕ *Nueva Planta*\n\nUsá:\n`/nueva_planta ID NOMBRE`"
        elif parametro == "usuarios":
            texto = "👥 *Usuarios*\n\n`/agregar_admin ID`\n`/agregar_operador ID`\n`/agregar_lector ID`\n`/listar_usuarios`"
        else:
            texto = "?"
        
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
        await update.message.reply_text("✅ Planta eliminada")
    else:
        await update.message.reply_text("❌ No encontrada")


@requiere_autorizacion
async def estadisticas_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = obtener_estadisticas_globales()
    texto = (
        f"📊 *Estadísticas*\n\n"
        f"📡 Total: *{stats['total_plantas']}*\n"
        f"🟢 Operando: *{stats['plantas_operando']}*\n"
        f"🧪 Pureza prom: *{stats['pureza_promedio']:.1f}%*\n"
        f"💨 Flujo total: *{stats['flujo_total']:.1f} Nm³/h*"
    )
    await update.message.reply_text(texto, parse_mode="Markdown")


@requiere_admin
async def exportar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("⚠️ Uso: `/exportar PLANTA_ID DIAS`", parse_mode="Markdown")
        return
    
    planta_id = context.args[0]
    dias = int(context.args[1])
    
    desde = (datetime.now() - timedelta(days=dias)).isoformat()
    datos = obtener_historial_db(planta_id, desde=desde)
    
    if not datos:
        await update.message.reply_text("📭 Sin datos")
        return
    
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=datos[0].keys())
    writer.writeheader()
    writer.writerows(datos)
    
    output.seek(0)
    await update.message.reply_document(
        document=io.BytesIO(output.getvalue().encode('utf-8')),
        filename=f"{planta_id}_{dias}d.csv",
        caption=f"📥 {len(datos)} registros"
    )


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
    await update.message.reply_text(f"✅ `{nuevo_id}` agregado", parse_mode="Markdown")


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
            await update.message.reply_text("✅ Eliminado")
            return
    
    await update.message.reply_text("⚠️ No encontrado")


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
    await update.message.reply_text(f"🆔 Tu ID: `{update.effective_user.id}`", parse_mode="Markdown")


async def ayuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = "`/start` - Menú\n`/mi_id` - Tu ID\n`/stats` - Estadísticas\n`/exportar PLANTA DIAS` - CSV"
    await update.message.reply_text(texto, parse_mode="Markdown")


# ================================================================================
# API FLASK
# ================================================================================

@flask_app.route("/api/datos", methods=["POST"])
def recibir_datos():
    api_key = request.headers.get("X-API-Key")
    if api_key != API_KEY:
        return jsonify({"error": "No autorizado"}), 401
    
    try:
        datos = request.get_json()
        if not datos or "planta_id" not in datos:
            return jsonify({"error": "Datos incompletos"}), 400
        
        planta_id = datos["planta_id"]
        logger.info(f"Datos: {planta_id}")
        
        plantas = obtener_plantas_db()
        alarma_anterior = plantas.get(planta_id, {}).get("alarma", False)
        
        actualizar_planta_db(planta_id, datos)
        
        if sistema_alertas and datos.get("alarma") and not alarma_anterior:
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            plantas_act = obtener_plantas_db()
            loop.run_until_complete(sistema_alertas.enviar_alerta(planta_id, plantas_act.get(planta_id, datos)))
        
        return jsonify({"status": "ok"}), 200
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route("/api/plantas", methods=["GET"])
def listar_plantas_api():
    api_key = request.headers.get("X-API-Key") or request.args.get("api_key")
    if api_key != API_KEY:
        return jsonify({"error": "No autorizado"}), 401
    
    plantas = obtener_plantas_db()
    for p in plantas.values():
        if p.get('ultima_actualizacion') and not isinstance(p['ultima_actualizacion'], str):
            p['ultima_actualizacion'] = p['ultima_actualizacion'].isoformat()
    
    return jsonify(plantas), 200


@flask_app.route("/api/historial_json", methods=["GET"])
def historial_json():
    api_key = request.args.get("api_key")
    if api_key != API_KEY:
        return jsonify({"error": "No autorizado"}), 401

    planta_id = request.args.get("planta_id")
    if not planta_id:
        return jsonify({"error": "Falta planta_id"}), 400

    desde = request.args.get("desde")
    hasta = request.args.get("hasta")
    
    datos = obtener_historial_db(planta_id, desde=desde, hasta=hasta)
    return jsonify(datos), 200


@flask_app.route("/api/estadisticas", methods=["GET"])
def estadisticas_api():
    api_key = request.args.get("api_key")
    if api_key != API_KEY:
        return jsonify({"error": "No autorizado"}), 401
    
    planta_id = request.args.get("planta_id")
    desde = request.args.get("desde")
    hasta = request.args.get("hasta")
    
    if planta_id:
        datos = obtener_historial_db(planta_id, desde=desde, hasta=hasta)
        stats = calcular_estadisticas(datos)
    else:
        stats = obtener_estadisticas_globales()
    
    return jsonify(stats), 200


@flask_app.route("/api/exportar_csv", methods=["GET"])
def exportar_csv_api():
    api_key = request.args.get("api_key")
    if api_key != API_KEY:
        return jsonify({"error": "No autorizado"}), 401
    
    planta_id = request.args.get("planta_id")
    desde = request.args.get("desde")
    hasta = request.args.get("hasta")
    
    if planta_id and planta_id.lower() != "all":
        datos = obtener_historial_db(planta_id, desde=desde, hasta=hasta)
    else:
        plantas = obtener_plantas_db()
        datos = []
        for pid in plantas:
            datos.extend(obtener_historial_db(pid, desde=desde, hasta=hasta))
    
    output = io.StringIO()
    if datos:
        writer = csv.DictWriter(output, fieldnames=datos[0].keys())
        writer.writeheader()
        writer.writerows(datos)
    
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=historial.csv"}
    )


@flask_app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "db": "postgresql"}), 200


@flask_app.route("/", methods=["GET"])
def home():
    plantas = obtener_plantas_db()
    return jsonify({
        "servicio": "Monitor Plantas O2 PSA",
        "version": "2.0-postgresql",
        "plantas": len(plantas),
        "database": "PostgreSQL"
    }), 200


# ================================================================================
# DASHBOARD
# ================================================================================

@flask_app.route("/dashboard", methods=["GET"])
def dashboard():
    api_key = request.args.get("api_key")
    if api_key != API_KEY:
        return "No autorizado - Usa ?api_key=TU_CLAVE", 401

    plantas = obtener_plantas_db()
    
    plantas_clean = {}
    for pid, p in plantas.items():
        plantas_clean[pid] = {k: (v.isoformat() if hasattr(v, 'isoformat') else v) for k, v in p.items()}

    if not plantas_clean:
        return "<html><body style='background:#0b1724;color:#fff;padding:50px;'><h2>No hay plantas</h2></body></html>"

    plantas_json = json.dumps(plantas_clean)

    html = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SCADA - Plantas O2</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:Arial,sans-serif;background:#0b1724;color:#ecf0f1;min-height:100vh}
        header{background:#111827;padding:15px 20px;display:flex;justify-content:space-between;align-items:center}
        header h1{font-size:18px;color:#3b82f6}
        .hdr-stats{display:flex;gap:15px;font-size:13px}
        .dot{width:10px;height:10px;border-radius:50%;display:inline-block;margin-right:5px}
        .dot-ok{background:#22c55e}.dot-warn{background:#eab308}.dot-danger{background:#ef4444}
        .container{display:grid;grid-template-columns:250px 1fr;gap:15px;padding:15px}
        .sidebar{background:#111827;border-radius:10px;padding:15px}
        .sidebar h2{font-size:14px;color:#9ca3af;margin-bottom:10px}
        .plant-list{list-style:none}
        .plant-item{padding:10px;margin-bottom:5px;border-radius:6px;cursor:pointer;display:flex;justify-content:space-between;align-items:center}
        .plant-item:hover{background:#1f2937}
        .plant-item.selected{background:#2563eb}
        .content{display:flex;flex-direction:column;gap:15px}
        .controls{background:#111827;border-radius:10px;padding:15px;display:flex;flex-wrap:wrap;gap:10px;align-items:center}
        .controls input,.controls select{background:#020617;border:1px solid #374151;border-radius:5px;padding:6px 10px;color:#fff;font-size:12px}
        .btn{background:#2563eb;border:none;border-radius:5px;padding:8px 15px;color:#fff;cursor:pointer;font-size:12px}
        .btn:hover{background:#1d4ed8}.btn-secondary{background:#374151}.btn-success{background:#059669}
        .metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}
        .metric-card{background:#111827;border-radius:10px;padding:15px;text-align:center}
        .metric-card h3{font-size:11px;color:#9ca3af;margin-bottom:5px}
        .metric-card .value{font-size:26px;font-weight:bold}
        .metric-card .unit{font-size:12px;color:#9ca3af}
        .value-ok{color:#22c55e}.value-warn{color:#eab308}.value-danger{color:#ef4444}
        .charts{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}
        .chart-card{background:#111827;border-radius:10px;padding:15px}
        .chart-card h3{font-size:13px;margin-bottom:10px}
        .chart-card canvas{max-height:180px}
        .kpis{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
        .kpi-card{background:#111827;border-radius:10px;padding:15px;text-align:center}
        .kpi-card h4{font-size:10px;color:#9ca3af;text-transform:uppercase}
        .kpi-card .kpi-value{font-size:30px;font-weight:bold;margin:8px 0}
        .kpi-bar{height:6px;background:#374151;border-radius:3px;overflow:hidden}
        .kpi-bar-fill{height:100%;border-radius:3px}
        @media(max-width:900px){.container{grid-template-columns:1fr}.charts{grid-template-columns:1fr}.kpis{grid-template-columns:1fr}}
    </style>
</head>
<body>
    <header>
        <h1>🏥 SCADA - Plantas O2 PSA</h1>
        <div class="hdr-stats">
            <span><span class="dot dot-ok"></span><span id="hdrOp">0</span> Op</span>
            <span><span class="dot dot-warn"></span><span id="hdrMant">0</span> Mant</span>
            <span><span class="dot dot-danger"></span><span id="hdrAlm">0</span> Alm</span>
            <span style="color:#9ca3af">🕐 <span id="hdrHora">--:--</span></span>
        </div>
    </header>
    <div class="container">
        <div class="sidebar">
            <h2>📡 PLANTAS</h2>
            <ul class="plant-list" id="plantList"></ul>
            <div style="margin-top:20px;border-top:1px solid #374151;padding-top:15px">
                <button class="btn btn-success" style="width:100%" onclick="exportarCSV()">📥 Descargar CSV</button>
            </div>
        </div>
        <div class="content">
            <div class="controls">
                <strong id="lblPlanta" style="color:#3b82f6">--</strong>
                <input type="datetime-local" id="filtroDesde">
                <input type="datetime-local" id="filtroHasta">
                <button class="btn" onclick="cargarDatos()">Aplicar</button>
                <button class="btn btn-secondary" onclick="setFiltro(1)">24h</button>
                <button class="btn btn-secondary" onclick="setFiltro(7)">7d</button>
                <button class="btn btn-secondary" onclick="setFiltro(30)">30d</button>
            </div>
            <div class="metrics">
                <div class="metric-card"><h3>🧪 PUREZA</h3><div class="value" id="metPureza">--</div><div class="unit">%</div></div>
                <div class="metric-card"><h3>💨 FLUJO</h3><div class="value" id="metFlujo">--</div><div class="unit">Nm³/h</div></div>
                <div class="metric-card"><h3>📈 PRESIÓN</h3><div class="value" id="metPresion">--</div><div class="unit">bar</div></div>
                <div class="metric-card"><h3>🌡️ TEMP</h3><div class="value" id="metTemp">--</div><div class="unit">°C</div></div>
                <div class="metric-card"><h3>⚙️ MODO</h3><div class="value" id="metModo" style="font-size:16px">--</div><div class="unit" id="metHoras">--</div></div>
            </div>
            <div class="kpis">
                <div class="kpi-card"><h4>Disponibilidad</h4><div class="kpi-value value-ok" id="kpiDisp">--%</div><div class="kpi-bar"><div class="kpi-bar-fill" id="kpiDispBar" style="background:#22c55e;width:0%"></div></div></div>
                <div class="kpi-card"><h4>Cumpl. Pureza</h4><div class="kpi-value" id="kpiPureza">--%</div><div class="kpi-bar"><div class="kpi-bar-fill" id="kpiPurezaBar" style="background:#3b82f6;width:0%"></div></div></div>
                <div class="kpi-card"><h4>Registros</h4><div class="kpi-value" id="kpiReg" style="color:#9ca3af">--</div></div>
            </div>
            <div class="charts">
                <div class="chart-card"><h3>🧪 Pureza (%)</h3><canvas id="chartPureza"></canvas></div>
                <div class="chart-card"><h3>💨 Flujo (Nm³/h)</h3><canvas id="chartFlujo"></canvas></div>
                <div class="chart-card"><h3>📈 Presión (bar)</h3><canvas id="chartPresion"></canvas></div>
                <div class="chart-card"><h3>🌡️ Temp (°C)</h3><canvas id="chartTemp"></canvas></div>
            </div>
        </div>
    </div>
    <script>
        const PLANTAS=""" + plantas_json + """;
        const API_KEY='""" + API_KEY + """';
        let plantaSel=null,charts={};
        
        function initCharts(){
            const cfg=(l,c)=>({type:'line',data:{labels:[],datasets:[{label:l,data:[],borderColor:c,borderWidth:2,tension:0.3,pointRadius:0,fill:false}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:'#9ca3af',maxTicksLimit:6}},y:{ticks:{color:'#9ca3af'}}}}});
            charts.pureza=new Chart(document.getElementById('chartPureza'),cfg('Pureza','#22c55e'));
            charts.flujo=new Chart(document.getElementById('chartFlujo'),cfg('Flujo','#3b82f6'));
            charts.presion=new Chart(document.getElementById('chartPresion'),cfg('Presión','#eab308'));
            charts.temp=new Chart(document.getElementById('chartTemp'),cfg('Temp','#ef4444'));
        }
        
        function updateChart(ch,labels,data){ch.data.labels=labels;ch.data.datasets[0].data=data;ch.update('none');}
        
        function crearLista(){
            const ul=document.getElementById('plantList');ul.innerHTML='';
            const ids=Object.keys(PLANTAS);if(!plantaSel&&ids.length)plantaSel=ids[0];
            let op=0,mant=0,alm=0;
            ids.forEach(id=>{
                const p=PLANTAS[id],li=document.createElement('li');
                li.className='plant-item'+(id===plantaSel?' selected':'');
                let dc='dot-ok';
                if(p.alarma){dc='dot-danger';alm++}else if(p.modo==='Mantenimiento'){dc='dot-warn';mant++}else{op++}
                li.innerHTML=`<span>${p.nombre||id}</span><span class="dot ${dc}"></span>`;
                li.onclick=()=>{plantaSel=id;crearLista();cargarDatos()};
                ul.appendChild(li);
            });
            document.getElementById('hdrOp').textContent=op;
            document.getElementById('hdrMant').textContent=mant;
            document.getElementById('hdrAlm').textContent=alm;
            document.getElementById('hdrHora').textContent=new Date().toLocaleTimeString('es-PY',{hour:'2-digit',minute:'2-digit'});
            document.getElementById('lblPlanta').textContent=plantaSel&&PLANTAS[plantaSel]?PLANTAS[plantaSel].nombre:'--';
        }
        
        function setFiltro(dias){
            const ahora=new Date(),desde=new Date(ahora.getTime()-dias*24*60*60*1000);
            const fmt=d=>d.toISOString().slice(0,16);
            document.getElementById('filtroDesde').value=fmt(desde);
            document.getElementById('filtroHasta').value=fmt(ahora);
            cargarDatos();
        }
        
        function cargarDatos(){
            if(!plantaSel)return;
            const p=PLANTAS[plantaSel];
            if(p){
                const pur=p.pureza_pct||0;
                document.getElementById('metPureza').textContent=pur.toFixed(1);
                document.getElementById('metPureza').className='value '+(pur>=93?'value-ok':pur>=90?'value-warn':'value-danger');
                document.getElementById('metFlujo').textContent=(p.flujo_nm3h||0).toFixed(1);
                document.getElementById('metPresion').textContent=(p.presion_bar||0).toFixed(1);
                document.getElementById('metTemp').textContent=(p.temperatura_c||0).toFixed(1);
                document.getElementById('metModo').textContent=p.modo||'--';
                document.getElementById('metModo').className='value '+(p.modo==='Producción'?'value-ok':'value-warn');
                document.getElementById('metHoras').textContent=(p.horas_operacion||0).toLocaleString()+' h';
            }
            const desde=document.getElementById('filtroDesde').value,hasta=document.getElementById('filtroHasta').value;
            let url=`/api/historial_json?api_key=${API_KEY}&planta_id=${plantaSel}`;
            if(desde)url+=`&desde=${desde}`;if(hasta)url+=`&hasta=${hasta}`;
            fetch(url).then(r=>r.json()).then(datos=>{
                if(!datos.length){
                    document.getElementById('kpiDisp').textContent='--%';
                    document.getElementById('kpiPureza').textContent='--%';
                    document.getElementById('kpiReg').textContent='0';
                    document.getElementById('kpiDispBar').style.width='0%';
                    document.getElementById('kpiPurezaBar').style.width='0%';
                    updateChart(charts.pureza,[],[]);updateChart(charts.flujo,[],[]);
                    updateChart(charts.presion,[],[]);updateChart(charts.temp,[],[]);
                    return;
                }
                const prod=datos.filter(d=>d.modo==='Producción').length,disp=(prod/datos.length*100);
                const purezaOk=datos.filter(d=>(d.pureza_pct||0)>=93).length,cumpl=(purezaOk/datos.length*100);
                document.getElementById('kpiDisp').textContent=disp.toFixed(1)+'%';
                document.getElementById('kpiDisp').className='kpi-value '+(disp>=90?'value-ok':'value-warn');
                document.getElementById('kpiDispBar').style.width=disp+'%';
                document.getElementById('kpiPureza').textContent=cumpl.toFixed(1)+'%';
                document.getElementById('kpiPureza').className='kpi-value '+(cumpl>=90?'value-ok':'value-warn');
                document.getElementById('kpiPurezaBar').style.width=cumpl+'%';
                document.getElementById('kpiReg').textContent=datos.length.toLocaleString();
                const labels=datos.map(d=>{const dt=new Date(d.timestamp);return dt.toLocaleString('es-PY',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'})});
                updateChart(charts.pureza,labels,datos.map(d=>d.pureza_pct||0));
                updateChart(charts.flujo,labels,datos.map(d=>d.flujo_nm3h||0));
                updateChart(charts.presion,labels,datos.map(d=>d.presion_bar||0));
                updateChart(charts.temp,labels,datos.map(d=>d.temperatura_c||0));
            }).catch(e=>console.error(e));
        }
        
        function exportarCSV(){
            if(!plantaSel)return alert('Seleccioná una planta');
            const desde=document.getElementById('filtroDesde').value,hasta=document.getElementById('filtroHasta').value;
            let url=`/api/exportar_csv?api_key=${API_KEY}&planta_id=${plantaSel}`;
            if(desde)url+=`&desde=${desde}`;if(hasta)url+=`&hasta=${hasta}`;
            window.location.href=url;
        }
        
        document.addEventListener('DOMContentLoaded',()=>{initCharts();crearLista();setFiltro(1)});
    </script>
</body>
</html>
    """
    return html


# ================================================================================
# MAIN
# ================================================================================

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)


def main():
    global sistema_alertas, USUARIOS
    
    print("=" * 50)
    print("  BOT PLANTAS O2 - POSTGRESQL")
    print("=" * 50)
    
    if not TELEGRAM_TOKEN:
        print("❌ Falta TELEGRAM_TOKEN")
        return
    
    if not ADMIN_PRINCIPAL_ID:
        print("❌ Falta ADMIN_PRINCIPAL_ID")
        return
    
    if not DATABASE_URL:
        print("❌ Falta DATABASE_URL")
        print("   Creá PostgreSQL en Render y conectalo")
        return
    
    print(f"✓ Token: OK")
    print(f"✓ Admin: {ADMIN_PRINCIPAL_ID}")
    print(f"✓ DB: PostgreSQL")
    
    try:
        inicializar_db()
        print("✓ DB inicializada")
    except Exception as e:
        print(f"❌ Error DB: {e}")
        return
    
    USUARIOS = cargar_usuarios()
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    sistema_alertas = SistemaAlertas(app)
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("mi_id", mi_id))
    app.add_handler(CommandHandler("ayuda", ayuda))
    app.add_handler(CommandHandler("stats", estadisticas_cmd))
    app.add_handler(CommandHandler("nueva_planta", nueva_planta))
    app.add_handler(CommandHandler("eliminar_planta", eliminar_planta))
    app.add_handler(CommandHandler("exportar", exportar_cmd))
    app.add_handler(CommandHandler("agregar_admin", agregar_admin))
    app.add_handler(CommandHandler("agregar_operador", agregar_operador))
    app.add_handler(CommandHandler("agregar_lector", agregar_lector))
    app.add_handler(CommandHandler("remover_usuario", remover_usuario))
    app.add_handler(CommandHandler("listar_usuarios", listar_usuarios))
    app.add_handler(CallbackQueryHandler(manejar_callback))
    
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print(f"✓ API puerto {PORT}")
    
    print("=" * 50)
    print("🚀 Bot iniciado!")
    print("=" * 50)
    
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
