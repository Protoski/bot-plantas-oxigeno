"""
================================================================================
Bot de Telegram para Monitoreo de Plantas de Oxígeno PSA
VERSIÓN MEJORADA CON DASHBOARD SCADA PROFESIONAL
================================================================================
Características:
- Dashboard SCADA con múltiples tipos de gráficos
- Estadísticas avanzadas (promedios, máx, mín, desviación estándar)
- Exportación de datos a CSV y Excel
- KPIs y métricas de rendimiento
- Comparativa entre plantas
- Histogramas de distribución
- Reportes automáticos
- Sistema de alertas mejorado
================================================================================
"""

import os
import json
import logging
import sqlite3
import threading
import csv
import io
from datetime import datetime, timedelta
from pathlib import Path
from functools import wraps
from typing import Optional, List, Dict, Any
from statistics import mean, stdev, median

from flask import Flask, request, jsonify, Response, send_file
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

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
ADMIN_PRINCIPAL_ID = int(os.environ.get("ADMIN_PRINCIPAL_ID", "0"))
API_KEY = os.environ.get("API_KEY", "clave_secreta_123")
PORT = int(os.environ.get("PORT", 5000))
DATABASE_FILE = os.environ.get("DATABASE_PATH", "/tmp/plantas_oxigeno.db")
USUARIOS_FILE = os.environ.get("USUARIOS_PATH", "/tmp/usuarios_autorizados.json")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
SPREADSHEET_NAME = os.environ.get("SPREADSHEET_NAME", "Monitoreo_Plantas_Oxigeno")

# Configuración de alertas
PUREZA_MINIMA = float(os.environ.get("PUREZA_MINIMA", "93.0"))
PRESION_MAXIMA = float(os.environ.get("PRESION_MAXIMA", "7.0"))
TEMPERATURA_MAXIMA = float(os.environ.get("TEMPERATURA_MAXIMA", "45.0"))

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
# BASE DE DATOS SQLITE MEJORADA
# ================================================================================

def inicializar_db():
    """Crea las tablas necesarias con índices optimizados."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    # Tabla de plantas
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
            activa INTEGER DEFAULT 1,
            capacidad_nominal REAL DEFAULT 0,
            fecha_instalacion TEXT,
            responsable TEXT DEFAULT ''
        )
    """)
    
    # Tabla de historial con índices
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
            mensaje_alarma TEXT,
            horas_operacion INTEGER DEFAULT 0
        )
    """)
    
    # Índices para mejor rendimiento
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_historial_planta ON historial(planta_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_historial_timestamp ON historial(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_historial_planta_timestamp ON historial(planta_id, timestamp)")
    
    # Tabla de configuración de alertas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS config_alertas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            planta_id TEXT UNIQUE,
            intervalo_alerta_min INTEGER DEFAULT 5,
            ultima_alerta TEXT,
            alertas_activas INTEGER DEFAULT 1,
            pureza_minima REAL DEFAULT 93.0,
            presion_maxima REAL DEFAULT 7.0,
            temperatura_maxima REAL DEFAULT 45.0,
            flujo_minimo REAL DEFAULT 0
        )
    """)
    
    # Tabla de eventos/logs
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS eventos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            planta_id TEXT,
            timestamp TEXT NOT NULL,
            tipo TEXT NOT NULL,
            descripcion TEXT,
            usuario_id INTEGER,
            datos_adicionales TEXT
        )
    """)
    
    # Tabla de mantenimientos programados
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mantenimientos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            planta_id TEXT NOT NULL,
            fecha_programada TEXT NOT NULL,
            tipo TEXT NOT NULL,
            descripcion TEXT,
            completado INTEGER DEFAULT 0,
            fecha_completado TEXT,
            responsable TEXT
        )
    """)
    
    # Tabla de reportes generados
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reportes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            planta_id TEXT,
            fecha_generacion TEXT NOT NULL,
            tipo TEXT NOT NULL,
            periodo_inicio TEXT,
            periodo_fin TEXT,
            datos_json TEXT
        )
    """)
    
    conn.commit()
    conn.close()
    logger.info("Base de datos inicializada con esquema mejorado")


def obtener_plantas_db() -> dict:
    """Obtiene todas las plantas activas."""
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
    
    # Registrar en historial
    cursor.execute("""
        INSERT INTO historial (planta_id, timestamp, presion_bar, temperatura_c,
                              pureza_pct, flujo_nm3h, modo, alarma, mensaje_alarma, horas_operacion)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    
    conn.commit()
    conn.close()


def agregar_planta_db(planta_id: str, nombre: str, ubicacion: str = "", 
                      capacidad: float = 0, responsable: str = "") -> bool:
    """Agrega una nueva planta con información extendida."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO plantas (id, nombre, ubicacion, capacidad_nominal, 
                                responsable, fecha_instalacion, ultima_actualizacion, activa)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
        """, (planta_id, nombre, ubicacion, capacidad, responsable, 
              datetime.now().isoformat(), datetime.now().isoformat()))
        
        cursor.execute("""
            INSERT INTO config_alertas (planta_id, intervalo_alerta_min, alertas_activas,
                                       pureza_minima, presion_maxima, temperatura_maxima)
            VALUES (?, 5, 1, ?, ?, ?)
        """, (planta_id, PUREZA_MINIMA, PRESION_MAXIMA, TEMPERATURA_MAXIMA))
        
        # Registrar evento
        cursor.execute("""
            INSERT INTO eventos (planta_id, timestamp, tipo, descripcion)
            VALUES (?, ?, 'CREACION', 'Planta registrada en el sistema')
        """, (planta_id, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False


def eliminar_planta_db(planta_id: str) -> bool:
    """Desactiva una planta (soft delete)."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("UPDATE plantas SET activa = 0 WHERE id = ?", (planta_id,))
    affected = cursor.rowcount
    
    if affected > 0:
        cursor.execute("""
            INSERT INTO eventos (planta_id, timestamp, tipo, descripcion)
            VALUES (?, ?, 'ELIMINACION', 'Planta desactivada del sistema')
        """, (planta_id, datetime.now().isoformat()))
    
    conn.commit()
    conn.close()
    return affected > 0


def obtener_historial_db(planta_id: str, desde: str = None, hasta: str = None, 
                         limite: int = None) -> List[Dict]:
    """Obtiene historial con filtros."""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query = """
        SELECT planta_id, timestamp, presion_bar, temperatura_c,
               pureza_pct, flujo_nm3h, modo, alarma, mensaje_alarma, horas_operacion
        FROM historial
        WHERE planta_id = ?
    """
    params = [planta_id]
    
    if desde:
        if len(desde) == 10:
            desde = desde + "T00:00:00"
        query += " AND timestamp >= ?"
        params.append(desde)
    
    if hasta:
        if len(hasta) == 10:
            hasta = hasta + "T23:59:59"
        query += " AND timestamp <= ?"
        params.append(hasta)
    
    query += " ORDER BY timestamp ASC"
    
    if limite:
        query += f" LIMIT {limite}"
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(r) for r in rows]


def calcular_estadisticas(datos: List[Dict]) -> Dict:
    """Calcula estadísticas completas de un conjunto de datos."""
    if not datos:
        return {}
    
    def safe_stats(values):
        values = [v for v in values if v is not None]
        if not values:
            return {"min": 0, "max": 0, "avg": 0, "median": 0, "std": 0, "count": 0}
        
        return {
            "min": round(min(values), 2),
            "max": round(max(values), 2),
            "avg": round(mean(values), 2),
            "median": round(median(values), 2),
            "std": round(stdev(values), 2) if len(values) > 1 else 0,
            "count": len(values)
        }
    
    pureza = [d.get("pureza_pct", 0) for d in datos]
    flujo = [d.get("flujo_nm3h", 0) for d in datos]
    presion = [d.get("presion_bar", 0) for d in datos]
    temperatura = [d.get("temperatura_c", 0) for d in datos]
    
    # Contar alarmas
    alarmas = sum(1 for d in datos if d.get("alarma"))
    
    # Tiempo en cada modo
    modos = {}
    for d in datos:
        modo = d.get("modo", "Desconocido")
        modos[modo] = modos.get(modo, 0) + 1
    
    # Calcular disponibilidad (tiempo en producción / tiempo total)
    tiempo_produccion = modos.get("Producción", 0)
    disponibilidad = (tiempo_produccion / len(datos) * 100) if datos else 0
    
    # Tiempo con pureza >= 93%
    pureza_aceptable = sum(1 for p in pureza if p >= 93)
    cumplimiento_pureza = (pureza_aceptable / len(pureza) * 100) if pureza else 0
    
    return {
        "periodo": {
            "inicio": datos[0].get("timestamp") if datos else None,
            "fin": datos[-1].get("timestamp") if datos else None,
            "registros": len(datos)
        },
        "pureza": safe_stats(pureza),
        "flujo": safe_stats(flujo),
        "presion": safe_stats(presion),
        "temperatura": safe_stats(temperatura),
        "alarmas": {
            "total": alarmas,
            "porcentaje": round(alarmas / len(datos) * 100, 2) if datos else 0
        },
        "modos": modos,
        "kpis": {
            "disponibilidad": round(disponibilidad, 2),
            "cumplimiento_pureza": round(cumplimiento_pureza, 2)
        }
    }


def obtener_estadisticas_globales() -> Dict:
    """Obtiene estadísticas globales de todas las plantas."""
    plantas = obtener_plantas_db()
    
    total_plantas = len(plantas)
    plantas_operando = sum(1 for p in plantas.values() if p.get("modo") == "Producción")
    plantas_mantenimiento = sum(1 for p in plantas.values() if p.get("modo") == "Mantenimiento")
    plantas_alarma = sum(1 for p in plantas.values() if p.get("alarma"))
    
    # Promedios globales actuales
    if plantas:
        pureza_promedio = mean([p.get("pureza_pct", 0) for p in plantas.values()])
        flujo_total = sum([p.get("flujo_nm3h", 0) for p in plantas.values()])
    else:
        pureza_promedio = 0
        flujo_total = 0
    
    return {
        "total_plantas": total_plantas,
        "plantas_operando": plantas_operando,
        "plantas_mantenimiento": plantas_mantenimiento,
        "plantas_alarma": plantas_alarma,
        "pureza_promedio": round(pureza_promedio, 2),
        "flujo_total": round(flujo_total, 2),
        "timestamp": datetime.now().isoformat()
    }


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
        try:
            ws = spreadsheet.worksheet("Historial")
        except:
            ws = spreadsheet.add_worksheet("Historial", rows=5000, cols=12)
            ws.append_row(["Timestamp", "Planta", "Presión", "Temp", "Pureza", 
                          "Flujo", "Modo", "Alarma", "Horas Op"])
        
        ws.append_row([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            planta_id,
            datos.get("presion_bar", 0),
            datos.get("temperatura_c", 0),
            datos.get("pureza_pct", 0),
            datos.get("flujo_nm3h", 0),
            datos.get("modo", ""),
            "SÍ" if datos.get("alarma") else "No",
            datos.get("horas_operacion", 0)
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
# SISTEMA DE ALERTAS MEJORADO
# ================================================================================

class SistemaAlertas:
    def __init__(self, bot_app):
        self.bot_app = bot_app
        self.ultima_alerta = {}
    
    def obtener_config(self, planta_id: str) -> Dict:
        conn = sqlite3.connect(DATABASE_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM config_alertas WHERE planta_id = ?", (planta_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return dict(row)
        return {
            "intervalo_alerta_min": 5,
            "alertas_activas": 1,
            "pureza_minima": PUREZA_MINIMA,
            "presion_maxima": PRESION_MAXIMA,
            "temperatura_maxima": TEMPERATURA_MAXIMA
        }
    
    def puede_enviar(self, planta_id: str) -> bool:
        config = self.obtener_config(planta_id)
        intervalo = config.get("intervalo_alerta_min", 5)
        
        if planta_id not in self.ultima_alerta:
            return True
        return datetime.now() - self.ultima_alerta[planta_id] >= timedelta(minutes=intervalo)
    
    def verificar_umbrales(self, planta_id: str, datos: dict) -> List[str]:
        """Verifica si los valores exceden los umbrales configurados."""
        config = self.obtener_config(planta_id)
        alertas = []
        
        pureza = datos.get("pureza_pct", 100)
        if pureza < config.get("pureza_minima", PUREZA_MINIMA):
            alertas.append(f"⚠️ Pureza baja: {pureza:.1f}% (mín: {config['pureza_minima']}%)")
        
        presion = datos.get("presion_bar", 0)
        if presion > config.get("presion_maxima", PRESION_MAXIMA):
            alertas.append(f"⚠️ Presión alta: {presion:.1f} bar (máx: {config['presion_maxima']} bar)")
        
        temp = datos.get("temperatura_c", 0)
        if temp > config.get("temperatura_maxima", TEMPERATURA_MAXIMA):
            alertas.append(f"⚠️ Temperatura alta: {temp:.1f}°C (máx: {config['temperatura_maxima']}°C)")
        
        return alertas
    
    async def enviar_alerta(self, planta_id: str, datos: dict, alertas_adicionales: List[str] = None):
        if not self.puede_enviar(planta_id):
            return
        
        config = self.obtener_config(planta_id)
        if not config.get("alertas_activas", 1):
            return
        
        usuarios = cargar_usuarios()
        todos = usuarios.get("admins", []) + usuarios.get("operadores", []) + usuarios.get("lectores", [])
        
        alertas_texto = "\n".join(alertas_adicionales) if alertas_adicionales else ""
        
        texto = (
            f"🚨 *ALERTA - {datos.get('nombre', planta_id)}*\n\n"
            f"{alertas_texto}\n"
            f"⚠️ {datos.get('mensaje_alarma', 'Alarma activa')}\n\n"
            f"📊 *Valores actuales:*\n"
            f"├ Presión: {datos.get('presion_bar', 0):.1f} bar\n"
            f"├ Temp: {datos.get('temperatura_c', 0):.1f} °C\n"
            f"├ Pureza: {datos.get('pureza_pct', 0):.1f}%\n"
            f"└ Flujo: {datos.get('flujo_nm3h', 0):.1f} Nm³/h\n\n"
            f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
        )
        
        for user_id in todos:
            try:
                await self.bot_app.bot.send_message(chat_id=user_id, text=texto, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Error enviando alerta a {user_id}: {e}")
        
        self.ultima_alerta[planta_id] = datetime.now()
        
        # Registrar evento
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO eventos (planta_id, timestamp, tipo, descripcion, datos_adicionales)
            VALUES (?, ?, 'ALERTA', ?, ?)
        """, (planta_id, datetime.now().isoformat(), 
              datos.get('mensaje_alarma', 'Alarma'), json.dumps(datos)))
        conn.commit()
        conn.close()


sistema_alertas: Optional[SistemaAlertas] = None


# ================================================================================
# FUNCIONES DE FORMATO
# ================================================================================

def formatear_estado_planta(planta: dict) -> str:
    estado = "🟢" if planta.get("modo") == "Producción" and not planta.get("alarma") else \
             "🟡" if planta.get("modo") == "Mantenimiento" else "🔴"
    
    # Indicadores de estado
    pureza = planta.get("pureza_pct", 0)
    pureza_icon = "✅" if pureza >= 93 else "⚠️"
    
    texto = [
        f"{estado} *{planta.get('nombre', 'Sin nombre')}*",
        f"📍 {planta.get('ubicacion', 'Sin ubicación')}",
        "",
        f"⚙️ Modo: *{planta.get('modo', '?')}*",
        f"🕐 Horas Op: *{planta.get('horas_operacion', 0):,}h*",
        "",
        f"🧪 Pureza: *{pureza:.1f}%* {pureza_icon}",
        f"💨 Flujo: *{planta.get('flujo_nm3h', 0):.1f} Nm³/h*",
        f"📈 Presión: *{planta.get('presion_bar', 0):.1f} bar*",
        f"🌡 Temp: *{planta.get('temperatura_c', 0):.1f}°C*",
        "",
    ]
    
    if planta.get("alarma"):
        texto.append(f"🚨 *ALERTA:* {planta.get('mensaje_alarma', '')}")
    else:
        texto.append("✅ Sin alarmas activas")
    
    # Última actualización
    ultima = planta.get("ultima_actualizacion", "")
    if ultima:
        try:
            dt = datetime.fromisoformat(ultima)
            texto.append(f"\n🔄 Actualizado: {dt.strftime('%d/%m %H:%M')}")
        except:
            pass
    
    return "\n".join(texto)


def formatear_estadisticas(stats: Dict, nombre_planta: str) -> str:
    """Formatea estadísticas para Telegram."""
    if not stats:
        return "📊 Sin datos para el período seleccionado"
    
    periodo = stats.get("periodo", {})
    kpis = stats.get("kpis", {})
    
    texto = [
        f"📊 *Estadísticas - {nombre_planta}*",
        "",
        f"📅 *Período:*",
        f"├ Desde: {periodo.get('inicio', 'N/A')[:16] if periodo.get('inicio') else 'N/A'}",
        f"├ Hasta: {periodo.get('fin', 'N/A')[:16] if periodo.get('fin') else 'N/A'}",
        f"└ Registros: {periodo.get('registros', 0):,}",
        "",
        f"🎯 *KPIs:*",
        f"├ Disponibilidad: *{kpis.get('disponibilidad', 0):.1f}%*",
        f"└ Cumpl. Pureza: *{kpis.get('cumplimiento_pureza', 0):.1f}%*",
        "",
        f"🧪 *Pureza O₂ (%):*",
        f"├ Mín: {stats.get('pureza', {}).get('min', 0):.1f}",
        f"├ Máx: {stats.get('pureza', {}).get('max', 0):.1f}",
        f"├ Prom: {stats.get('pureza', {}).get('avg', 0):.1f}",
        f"└ Desv: ±{stats.get('pureza', {}).get('std', 0):.2f}",
        "",
        f"💨 *Flujo (Nm³/h):*",
        f"├ Mín: {stats.get('flujo', {}).get('min', 0):.1f}",
        f"├ Máx: {stats.get('flujo', {}).get('max', 0):.1f}",
        f"└ Prom: {stats.get('flujo', {}).get('avg', 0):.1f}",
        "",
        f"🚨 *Alarmas:* {stats.get('alarmas', {}).get('total', 0)} ({stats.get('alarmas', {}).get('porcentaje', 0):.1f}%)"
    ]
    
    return "\n".join(texto)


# ================================================================================
# HANDLERS DE TELEGRAM
# ================================================================================

@requiere_autorizacion
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    plantas = obtener_plantas_db()
    stats_globales = obtener_estadisticas_globales()
    
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
        InlineKeyboardButton("📈 Estadísticas", callback_data="stats:global")
    ])
    
    if es_admin(user.id):
        keyboard.append([
            InlineKeyboardButton("➕ Nueva Planta", callback_data="admin:agregar"),
            InlineKeyboardButton("👥 Usuarios", callback_data="admin:usuarios"),
        ])
        keyboard.append([
            InlineKeyboardButton("⚙️ Configuración", callback_data="admin:config"),
            InlineKeyboardButton("📥 Exportar", callback_data="admin:exportar")
        ])
    
    texto = (
        f"👋 Hola *{user.first_name}*!\n\n"
        f"🔑 Rol: *{obtener_rol(user.id).upper()}*\n\n"
        f"📡 *Estado General:*\n"
        f"├ Total plantas: *{stats_globales['total_plantas']}*\n"
        f"├ En producción: *{stats_globales['plantas_operando']}* 🟢\n"
        f"├ Mantenimiento: *{stats_globales['plantas_mantenimiento']}* 🟡\n"
        f"├ Con alarma: *{stats_globales['plantas_alarma']}* 🔴\n"
        f"├ Pureza prom: *{stats_globales['pureza_promedio']:.1f}%*\n"
        f"└ Flujo total: *{stats_globales['flujo_total']:.1f} Nm³/h*\n\n"
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
        keyboard = [
            [
                InlineKeyboardButton("🔁 Actualizar", callback_data=f"ver:{parametro}"),
                InlineKeyboardButton("📈 Stats 24h", callback_data=f"stats24:{parametro}")
            ]
        ]
        if es_operador_o_admin(user.id):
            keyboard.append([
                InlineKeyboardButton("🔄 Cambiar modo", callback_data=f"modo:{parametro}"),
                InlineKeyboardButton("📊 Stats 7d", callback_data=f"stats7d:{parametro}")
            ])
        keyboard.append([InlineKeyboardButton("⬅️ Menú", callback_data="menu:0")])
        await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "modo" and parametro in plantas:
        if es_operador_o_admin(user.id):
            nuevo = "Mantenimiento" if plantas[parametro].get("modo") == "Producción" else "Producción"
            actualizar_planta_db(parametro, {"modo": nuevo})
            plantas = obtener_plantas_db()
            texto = f"✅ Modo cambiado a: *{nuevo}*\n\n" + formatear_estado_planta(plantas[parametro])
            keyboard = [
                [InlineKeyboardButton("🔁 Actualizar", callback_data=f"ver:{parametro}"),
                 InlineKeyboardButton("🔄 Cambiar modo", callback_data=f"modo:{parametro}")],
                [InlineKeyboardButton("⬅️ Menú", callback_data="menu:0")]
            ]
            await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "stats24" and parametro in plantas:
        desde = (datetime.now() - timedelta(hours=24)).isoformat()
        datos = obtener_historial_db(parametro, desde=desde)
        stats = calcular_estadisticas(datos)
        texto = formatear_estadisticas(stats, plantas[parametro].get("nombre", parametro))
        keyboard = [
            [InlineKeyboardButton("📊 Stats 7d", callback_data=f"stats7d:{parametro}")],
            [InlineKeyboardButton("⬅️ Volver", callback_data=f"ver:{parametro}")]
        ]
        await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "stats7d" and parametro in plantas:
        desde = (datetime.now() - timedelta(days=7)).isoformat()
        datos = obtener_historial_db(parametro, desde=desde)
        stats = calcular_estadisticas(datos)
        texto = formatear_estadisticas(stats, plantas[parametro].get("nombre", parametro))
        keyboard = [
            [InlineKeyboardButton("📊 Stats 30d", callback_data=f"stats30d:{parametro}")],
            [InlineKeyboardButton("⬅️ Volver", callback_data=f"ver:{parametro}")]
        ]
        await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "stats30d" and parametro in plantas:
        desde = (datetime.now() - timedelta(days=30)).isoformat()
        datos = obtener_historial_db(parametro, desde=desde)
        stats = calcular_estadisticas(datos)
        texto = formatear_estadisticas(stats, plantas[parametro].get("nombre", parametro))
        keyboard = [[InlineKeyboardButton("⬅️ Volver", callback_data=f"ver:{parametro}")]]
        await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "stats" and parametro == "global":
        stats = obtener_estadisticas_globales()
        texto = (
            f"📊 *Estadísticas Globales*\n\n"
            f"📡 Total plantas: *{stats['total_plantas']}*\n"
            f"🟢 En producción: *{stats['plantas_operando']}*\n"
            f"🟡 En mantenimiento: *{stats['plantas_mantenimiento']}*\n"
            f"🔴 Con alarma: *{stats['plantas_alarma']}*\n\n"
            f"🧪 Pureza promedio: *{stats['pureza_promedio']:.1f}%*\n"
            f"💨 Flujo total: *{stats['flujo_total']:.1f} Nm³/h*\n\n"
            f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        )
        keyboard = [[InlineKeyboardButton("⬅️ Menú", callback_data="menu:0")]]
        await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "resumen":
        lineas = ["📊 *Resumen de Plantas*\n"]
        for pid, p in plantas.items():
            estado = "🟢" if p.get("modo") == "Producción" and not p.get("alarma") else \
                     "🟡" if p.get("modo") == "Mantenimiento" else "🔴"
            lineas.append(f"{estado} *{p.get('nombre', pid)}*")
            lineas.append(f"   ├ Modo: {p.get('modo', '?')}")
            lineas.append(f"   ├ Pureza: {p.get('pureza_pct', 0):.1f}%")
            lineas.append(f"   └ Flujo: {p.get('flujo_nm3h', 0):.1f} Nm³/h")
            lineas.append("")
        keyboard = [[InlineKeyboardButton("⬅️ Menú", callback_data="menu:0")]]
        await query.edit_message_text("\n".join(lineas), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    elif accion == "menu":
        await start(update, context)
    
    elif accion == "admin":
        if not es_admin(user.id):
            await query.answer("🔐 Solo admin", show_alert=True)
            return
        
        if parametro == "agregar":
            texto = (
                "➕ *Nueva Planta*\n\n"
                "Usá el comando:\n"
                "`/nueva_planta ID NOMBRE`\n\n"
                "O con más detalles:\n"
                "`/nueva_planta ID NOMBRE | UBICACION | CAPACIDAD`\n\n"
                "Ejemplo:\n"
                "`/nueva_planta planta_3 Hospital Sur | Asunción | 50`"
            )
        elif parametro == "usuarios":
            texto = (
                "👥 *Gestión de Usuarios*\n\n"
                "*Comandos:*\n"
                "`/agregar_admin ID`\n"
                "`/agregar_operador ID`\n"
                "`/agregar_lector ID`\n"
                "`/remover_usuario ID`\n"
                "`/listar_usuarios`"
            )
        elif parametro == "config":
            texto = (
                "⚙️ *Configuración*\n\n"
                "*Comandos:*\n"
                "`/config_alertas PLANTA_ID INTERVALO_MIN`\n"
                "`/config_umbrales PLANTA_ID PUREZA_MIN PRESION_MAX TEMP_MAX`\n\n"
                "Ejemplo:\n"
                "`/config_alertas planta_1 10`\n"
                "`/config_umbrales planta_1 93 7 45`"
            )
        elif parametro == "exportar":
            texto = (
                "📥 *Exportar Datos*\n\n"
                "Usá el comando:\n"
                "`/exportar PLANTA_ID DIAS`\n\n"
                "Ejemplo:\n"
                "`/exportar planta_1 7` (últimos 7 días)\n"
                "`/exportar all 30` (todas, 30 días)\n\n"
                "También podés usar el dashboard web."
            )
        else:
            texto = "❓ Opción no reconocida"
        
        keyboard = [[InlineKeyboardButton("⬅️ Volver", callback_data="menu:0")]]
        await query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


# ================================================================================
# COMANDOS DE TELEGRAM
# ================================================================================

@requiere_admin
async def nueva_planta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text(
            "⚠️ Uso:\n"
            "`/nueva_planta ID NOMBRE`\n"
            "o\n"
            "`/nueva_planta ID NOMBRE | UBICACION | CAPACIDAD`",
            parse_mode="Markdown"
        )
        return
    
    planta_id = context.args[0].lower().replace(" ", "_")
    resto = " ".join(context.args[1:])
    
    if "|" in resto:
        partes = [p.strip() for p in resto.split("|")]
        nombre = partes[0]
        ubicacion = partes[1] if len(partes) > 1 else ""
        capacidad = float(partes[2]) if len(partes) > 2 else 0
    else:
        nombre = resto
        ubicacion = ""
        capacidad = 0
    
    if agregar_planta_db(planta_id, nombre, ubicacion, capacidad):
        await update.message.reply_text(
            f"✅ Planta agregada:\n"
            f"├ ID: `{planta_id}`\n"
            f"├ Nombre: {nombre}\n"
            f"├ Ubicación: {ubicacion or 'N/A'}\n"
            f"└ Capacidad: {capacidad} Nm³/h",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(f"❌ Ya existe `{planta_id}`", parse_mode="Markdown")


@requiere_admin
async def eliminar_planta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ Uso: `/eliminar_planta ID`", parse_mode="Markdown")
        return
    
    if eliminar_planta_db(context.args[0]):
        await update.message.reply_text("✅ Planta eliminada", parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ No encontrada", parse_mode="Markdown")


@requiere_admin
async def config_alertas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text(
            "⚠️ Uso: `/config_alertas PLANTA_ID INTERVALO_MIN`",
            parse_mode="Markdown"
        )
        return
    
    planta_id = context.args[0]
    try:
        intervalo = int(context.args[1])
    except:
        await update.message.reply_text("❌ Intervalo debe ser un número")
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE config_alertas SET intervalo_alerta_min = ? WHERE planta_id = ?
    """, (intervalo, planta_id))
    
    if cursor.rowcount == 0:
        cursor.execute("""
            INSERT INTO config_alertas (planta_id, intervalo_alerta_min) VALUES (?, ?)
        """, (planta_id, intervalo))
    
    conn.commit()
    conn.close()
    
    await update.message.reply_text(
        f"✅ Configuración actualizada:\n"
        f"├ Planta: `{planta_id}`\n"
        f"└ Intervalo alertas: {intervalo} min",
        parse_mode="Markdown"
    )


@requiere_admin
async def config_umbrales(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 4:
        await update.message.reply_text(
            "⚠️ Uso: `/config_umbrales PLANTA_ID PUREZA_MIN PRESION_MAX TEMP_MAX`\n"
            "Ejemplo: `/config_umbrales planta_1 93 7 45`",
            parse_mode="Markdown"
        )
        return
    
    planta_id = context.args[0]
    try:
        pureza_min = float(context.args[1])
        presion_max = float(context.args[2])
        temp_max = float(context.args[3])
    except:
        await update.message.reply_text("❌ Los valores deben ser números")
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE config_alertas 
        SET pureza_minima = ?, presion_maxima = ?, temperatura_maxima = ?
        WHERE planta_id = ?
    """, (pureza_min, presion_max, temp_max, planta_id))
    
    if cursor.rowcount == 0:
        cursor.execute("""
            INSERT INTO config_alertas (planta_id, pureza_minima, presion_maxima, temperatura_maxima)
            VALUES (?, ?, ?, ?)
        """, (planta_id, pureza_min, presion_max, temp_max))
    
    conn.commit()
    conn.close()
    
    await update.message.reply_text(
        f"✅ Umbrales actualizados:\n"
        f"├ Planta: `{planta_id}`\n"
        f"├ Pureza mín: {pureza_min}%\n"
        f"├ Presión máx: {presion_max} bar\n"
        f"└ Temp máx: {temp_max}°C",
        parse_mode="Markdown"
    )


@requiere_autorizacion
async def estadisticas_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /stats para obtener estadísticas."""
    if not context.args:
        # Estadísticas globales
        stats = obtener_estadisticas_globales()
        texto = (
            f"📊 *Estadísticas Globales*\n\n"
            f"📡 Total plantas: *{stats['total_plantas']}*\n"
            f"🟢 En producción: *{stats['plantas_operando']}*\n"
            f"🟡 En mantenimiento: *{stats['plantas_mantenimiento']}*\n"
            f"🔴 Con alarma: *{stats['plantas_alarma']}*\n\n"
            f"🧪 Pureza promedio: *{stats['pureza_promedio']:.1f}%*\n"
            f"💨 Flujo total: *{stats['flujo_total']:.1f} Nm³/h*"
        )
    else:
        planta_id = context.args[0]
        dias = int(context.args[1]) if len(context.args) > 1 else 7
        
        plantas = obtener_plantas_db()
        if planta_id not in plantas:
            await update.message.reply_text("❌ Planta no encontrada")
            return
        
        desde = (datetime.now() - timedelta(days=dias)).isoformat()
        datos = obtener_historial_db(planta_id, desde=desde)
        stats = calcular_estadisticas(datos)
        texto = formatear_estadisticas(stats, plantas[planta_id].get("nombre", planta_id))
    
    await update.message.reply_text(texto, parse_mode="Markdown")


@requiere_admin
async def exportar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /exportar para exportar datos a CSV."""
    if len(context.args) < 2:
        await update.message.reply_text(
            "⚠️ Uso: `/exportar PLANTA_ID DIAS`\n"
            "Ejemplo: `/exportar planta_1 7`",
            parse_mode="Markdown"
        )
        return
    
    planta_id = context.args[0]
    try:
        dias = int(context.args[1])
    except:
        await update.message.reply_text("❌ Días debe ser un número")
        return
    
    desde = (datetime.now() - timedelta(days=dias)).isoformat()
    
    if planta_id.lower() == "all":
        plantas = obtener_plantas_db()
        todos_datos = []
        for pid in plantas:
            datos = obtener_historial_db(pid, desde=desde)
            todos_datos.extend(datos)
        datos = todos_datos
        nombre_archivo = f"todas_plantas_{dias}d.csv"
    else:
        datos = obtener_historial_db(planta_id, desde=desde)
        nombre_archivo = f"{planta_id}_{dias}d.csv"
    
    if not datos:
        await update.message.reply_text("📭 Sin datos para el período")
        return
    
    # Crear CSV
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "planta_id", "timestamp", "presion_bar", "temperatura_c",
        "pureza_pct", "flujo_nm3h", "modo", "alarma", "mensaje_alarma"
    ])
    writer.writeheader()
    writer.writerows(datos)
    
    # Enviar archivo
    output.seek(0)
    await update.message.reply_document(
        document=io.BytesIO(output.getvalue().encode('utf-8')),
        filename=nombre_archivo,
        caption=f"📥 Exportación: {len(datos)} registros"
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
            await update.message.reply_text("✅ Usuario eliminado")
            return
    
    await update.message.reply_text("⚠️ No encontrado")


@requiere_admin
async def listar_usuarios(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = ["👥 *Usuarios Autorizados*\n"]
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


async def ayuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra ayuda de comandos."""
    rol = obtener_rol(update.effective_user.id)
    
    texto = [
        "📖 *Comandos Disponibles*\n",
        "*Generales:*",
        "`/start` - Menú principal",
        "`/mi_id` - Ver tu ID de Telegram",
        "`/stats [planta] [dias]` - Estadísticas",
        "`/ayuda` - Esta ayuda",
    ]
    
    if rol in ["admin", "operador"]:
        texto.extend([
            "",
            "*Operador:*",
            "(Cambio de modo desde menú)",
        ])
    
    if rol == "admin":
        texto.extend([
            "",
            "*Admin:*",
            "`/nueva_planta ID NOMBRE`",
            "`/eliminar_planta ID`",
            "`/agregar_admin ID`",
            "`/agregar_operador ID`",
            "`/agregar_lector ID`",
            "`/remover_usuario ID`",
            "`/listar_usuarios`",
            "`/config_alertas PLANTA INTERVALO`",
            "`/config_umbrales PLANTA P T M`",
            "`/exportar PLANTA DIAS`",
        ])
    
    await update.message.reply_text("\n".join(texto), parse_mode="Markdown")


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
        
        plantas = obtener_plantas_db()
        alarma_anterior = plantas.get(planta_id, {}).get("alarma", False)
        
        actualizar_planta_db(planta_id, datos)
        registrar_en_sheets(planta_id, datos)
        
        # Verificar umbrales y enviar alertas
        if sistema_alertas:
            alertas = sistema_alertas.verificar_umbrales(planta_id, datos)
            if datos.get("alarma") and not alarma_anterior:
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                except:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                plantas_actualizado = obtener_plantas_db()
                datos_completos = plantas_actualizado.get(planta_id, datos)
                loop.run_until_complete(sistema_alertas.enviar_alerta(planta_id, datos_completos, alertas))
        
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
    """Devuelve historial en JSON para el dashboard."""
    api_key = request.args.get("api_key")
    if api_key != API_KEY:
        return jsonify({"error": "No autorizado"}), 401

    planta_id = request.args.get("planta_id")
    if not planta_id:
        return jsonify({"error": "Falta planta_id"}), 400

    desde = request.args.get("desde")
    hasta = request.args.get("hasta")
    limite = request.args.get("limite")
    
    datos = obtener_historial_db(
        planta_id, 
        desde=desde, 
        hasta=hasta, 
        limite=int(limite) if limite else None
    )
    
    return jsonify(datos), 200


@flask_app.route("/api/estadisticas", methods=["GET"])
def estadisticas_api():
    """Devuelve estadísticas calculadas."""
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
    """Exporta datos a CSV."""
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
        headers={"Content-Disposition": f"attachment; filename=historial_{datetime.now().strftime('%Y%m%d')}.csv"}
    )


@flask_app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()}), 200


@flask_app.route("/", methods=["GET"])
def home():
    """Página principal con info del servicio."""
    plantas = obtener_plantas_db()
    return jsonify({
        "servicio": "Monitor Plantas O2 PSA - SCADA",
        "version": "2.0",
        "estado": "activo",
        "plantas_registradas": len(plantas),
        "endpoints": {
            "POST /api/datos": "Recibir datos de ESP32",
            "GET /api/plantas": "Listar plantas",
            "GET /api/planta/<id>": "Detalle de planta",
            "GET /api/historial_json": "Historial de planta",
            "GET /api/estadisticas": "Estadísticas calculadas",
            "GET /api/exportar_csv": "Exportar a CSV",
            "GET /dashboard": "Dashboard SCADA web",
            "GET /health": "Estado del servicio"
        }
    }), 200


# ================================================================================
# DASHBOARD HTML MEJORADO
# ================================================================================

@flask_app.route("/dashboard", methods=["GET"])
def dashboard():
    api_key = request.args.get("api_key")
    if api_key != API_KEY:
        return "No autorizado", 401

    plantas = obtener_plantas_db()

    if not plantas:
        return """
        <html><head><title>SCADA - Sin datos</title></head><body style="background:#0b1724;color:#fff;font-family:Arial;">
        <h2>No hay plantas registradas aún.</h2>
        <p>Esperá a que el ESP32 envíe datos o agrega una planta desde Telegram.</p>
        </body></html>
        """

    plantas_json = json.dumps(plantas)

    html = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>SCADA - Plantas de Oxígeno PSA</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns"></script>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            body {
                font-family: 'Inter', Arial, sans-serif;
                background: linear-gradient(135deg, #0b1724 0%, #1a2634 100%);
                color: #ecf0f1;
                min-height: 100vh;
            }
            header {
                background: rgba(17, 24, 39, 0.95);
                padding: 12px 20px;
                display: flex;
                justify-content: space-between;
                align-items: center;
                box-shadow: 0 4px 20px rgba(0,0,0,0.3);
                position: sticky;
                top: 0;
                z-index: 100;
                backdrop-filter: blur(10px);
            }
            header h1 {
                font-size: 18px;
                font-weight: 600;
                background: linear-gradient(90deg, #3b82f6, #60a5fa);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }
            .header-status {
                display: flex;
                gap: 15px;
                font-size: 12px;
            }
            .header-status .stat {
                display: flex;
                align-items: center;
                gap: 5px;
            }
            .container {
                display: grid;
                grid-template-columns: 280px 1fr;
                gap: 15px;
                padding: 15px;
                max-width: 1800px;
                margin: 0 auto;
            }
            .sidebar {
                background: rgba(17, 24, 39, 0.8);
                border-radius: 12px;
                padding: 15px;
                backdrop-filter: blur(10px);
                border: 1px solid rgba(255,255,255,0.1);
            }
            .sidebar h2 {
                font-size: 14px;
                margin-bottom: 12px;
                color: #9ca3af;
                text-transform: uppercase;
                letter-spacing: 1px;
            }
            .plant-list {
                list-style: none;
                max-height: 350px;
                overflow-y: auto;
            }
            .plant-list::-webkit-scrollbar {
                width: 6px;
            }
            .plant-list::-webkit-scrollbar-thumb {
                background: #374151;
                border-radius: 3px;
            }
            .plant-item {
                padding: 10px 12px;
                margin-bottom: 6px;
                border-radius: 8px;
                cursor: pointer;
                display: flex;
                justify-content: space-between;
                align-items: center;
                font-size: 13px;
                transition: all 0.2s ease;
                border: 1px solid transparent;
            }
            .plant-item:hover {
                background: rgba(37, 99, 235, 0.2);
                border-color: rgba(37, 99, 235, 0.3);
            }
            .plant-item.selected {
                background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
                border-color: #3b82f6;
            }
            .estado-dot {
                width: 10px;
                height: 10px;
                border-radius: 50%;
                box-shadow: 0 0 8px currentColor;
            }
            .dot-ok { background: #22c55e; color: #22c55e; }
            .dot-mantenimiento { background: #eab308; color: #eab308; }
            .dot-alarma { background: #ef4444; color: #ef4444; animation: pulse 1.5s infinite; }
            
            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.5; }
            }

            .content {
                display: flex;
                flex-direction: column;
                gap: 15px;
            }
            
            /* Panel de controles */
            .controls-panel {
                background: rgba(17, 24, 39, 0.8);
                border-radius: 12px;
                padding: 15px;
                display: flex;
                flex-wrap: wrap;
                gap: 15px;
                align-items: center;
                border: 1px solid rgba(255,255,255,0.1);
            }
            .control-group {
                display: flex;
                align-items: center;
                gap: 8px;
            }
            .control-group label {
                font-size: 12px;
                color: #9ca3af;
            }
            .control-group input, .control-group select {
                background: #020617;
                border: 1px solid #374151;
                border-radius: 6px;
                padding: 6px 10px;
                color: #e5e7eb;
                font-size: 12px;
            }
            .control-group input:focus, .control-group select:focus {
                outline: none;
                border-color: #3b82f6;
            }
            .btn {
                background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
                border: none;
                border-radius: 6px;
                padding: 8px 16px;
                color: white;
                cursor: pointer;
                font-size: 12px;
                font-weight: 500;
                transition: all 0.2s ease;
            }
            .btn:hover {
                transform: translateY(-1px);
                box-shadow: 0 4px 12px rgba(37, 99, 235, 0.4);
            }
            .btn-secondary {
                background: #374151;
            }
            .btn-success {
                background: linear-gradient(135deg, #059669 0%, #047857 100%);
            }

            /* Tarjetas de métricas */
            .metrics-row {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 12px;
            }
            .metric-card {
                background: rgba(17, 24, 39, 0.8);
                border-radius: 12px;
                padding: 15px;
                border: 1px solid rgba(255,255,255,0.1);
                transition: all 0.3s ease;
            }
            .metric-card:hover {
                transform: translateY(-2px);
                border-color: rgba(37, 99, 235, 0.3);
            }
            .metric-card h3 {
                font-size: 11px;
                color: #9ca3af;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                margin-bottom: 8px;
            }
            .metric-card .valor {
                font-size: 28px;
                font-weight: 700;
            }
            .metric-card .unidad {
                font-size: 14px;
                color: #9ca3af;
                margin-left: 4px;
            }
            .metric-card .subtexto {
                font-size: 11px;
                margin-top: 6px;
            }
            .metric-card .trend {
                display: inline-flex;
                align-items: center;
                gap: 3px;
                padding: 2px 6px;
                border-radius: 4px;
                font-size: 10px;
            }
            .trend-up { background: rgba(34, 197, 94, 0.2); color: #22c55e; }
            .trend-down { background: rgba(239, 68, 68, 0.2); color: #ef4444; }
            .trend-stable { background: rgba(156, 163, 175, 0.2); color: #9ca3af; }
            
            .valor-ok { color: #22c55e; }
            .valor-warning { color: #eab308; }
            .valor-danger { color: #ef4444; }

            /* Tarjeta de alarma */
            .alarma-card {
                background: linear-gradient(135deg, rgba(239, 68, 68, 0.2) 0%, rgba(185, 28, 28, 0.2) 100%);
                border: 1px solid #ef4444;
                border-radius: 12px;
                padding: 15px;
                display: flex;
                align-items: center;
                gap: 15px;
                animation: alarma-pulse 2s infinite;
            }
            @keyframes alarma-pulse {
                0%, 100% { box-shadow: 0 0 20px rgba(239, 68, 68, 0.3); }
                50% { box-shadow: 0 0 30px rgba(239, 68, 68, 0.5); }
            }
            .alarma-card .icon {
                font-size: 32px;
            }
            .alarma-card .info h3 {
                color: #ef4444;
                font-size: 14px;
            }
            .alarma-card .info p {
                font-size: 12px;
                color: #fca5a5;
            }

            /* Panel de estadísticas */
            .stats-panel {
                background: rgba(17, 24, 39, 0.8);
                border-radius: 12px;
                padding: 15px;
                border: 1px solid rgba(255,255,255,0.1);
            }
            .stats-panel h3 {
                font-size: 14px;
                margin-bottom: 12px;
                color: #9ca3af;
            }
            .stats-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
                gap: 10px;
            }
            .stat-item {
                text-align: center;
                padding: 10px;
                background: rgba(0,0,0,0.2);
                border-radius: 8px;
            }
            .stat-item .label {
                font-size: 10px;
                color: #9ca3af;
                text-transform: uppercase;
            }
            .stat-item .value {
                font-size: 18px;
                font-weight: 600;
                margin-top: 4px;
            }

            /* Gráficos */
            .charts-container {
                display: grid;
                grid-template-columns: repeat(2, 1fr);
                gap: 12px;
            }
            .chart-card {
                background: rgba(17, 24, 39, 0.8);
                border-radius: 12px;
                padding: 15px;
                border: 1px solid rgba(255,255,255,0.1);
            }
            .chart-card.full-width {
                grid-column: span 2;
            }
            .chart-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 10px;
            }
            .chart-header h3 {
                font-size: 13px;
                color: #e5e7eb;
            }
            .chart-header select {
                background: #020617;
                border: 1px solid #374151;
                border-radius: 4px;
                padding: 4px 8px;
                color: #e5e7eb;
                font-size: 11px;
            }
            .chart-card canvas {
                max-height: 220px;
            }

            /* KPIs */
            .kpi-row {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 12px;
            }
            .kpi-card {
                background: rgba(17, 24, 39, 0.8);
                border-radius: 12px;
                padding: 15px;
                border: 1px solid rgba(255,255,255,0.1);
                text-align: center;
            }
            .kpi-card h4 {
                font-size: 11px;
                color: #9ca3af;
                text-transform: uppercase;
                margin-bottom: 8px;
            }
            .kpi-value {
                font-size: 36px;
                font-weight: 700;
            }
            .kpi-bar {
                height: 6px;
                background: #374151;
                border-radius: 3px;
                margin-top: 10px;
                overflow: hidden;
            }
            .kpi-bar-fill {
                height: 100%;
                border-radius: 3px;
                transition: width 0.5s ease;
            }

            .footer {
                text-align: center;
                font-size: 11px;
                color: #6b7280;
                padding: 15px;
                margin-top: auto;
            }

            /* Responsive */
            @media (max-width: 1200px) {
                .charts-container {
                    grid-template-columns: 1fr;
                }
                .chart-card.full-width {
                    grid-column: span 1;
                }
            }
            @media (max-width: 900px) {
                .container {
                    grid-template-columns: 1fr;
                }
                .sidebar {
                    order: 2;
                }
                .metrics-row {
                    grid-template-columns: repeat(2, 1fr);
                }
            }
            @media (max-width: 600px) {
                .metrics-row {
                    grid-template-columns: 1fr;
                }
                .controls-panel {
                    flex-direction: column;
                    align-items: stretch;
                }
            }

            /* Tabs */
            .tabs {
                display: flex;
                gap: 5px;
                margin-bottom: 15px;
                background: rgba(0,0,0,0.2);
                padding: 5px;
                border-radius: 8px;
            }
            .tab {
                padding: 8px 16px;
                border-radius: 6px;
                cursor: pointer;
                font-size: 12px;
                transition: all 0.2s ease;
                border: none;
                background: transparent;
                color: #9ca3af;
            }
            .tab:hover {
                background: rgba(255,255,255,0.1);
            }
            .tab.active {
                background: #2563eb;
                color: white;
            }
            .tab-content {
                display: none;
            }
            .tab-content.active {
                display: block;
            }
        </style>
    </head>
    <body>
        <header>
            <h1>🏥 SCADA - Monitor de Plantas de Oxígeno PSA</h1>
            <div class="header-status">
                <div class="stat">
                    <span class="estado-dot dot-ok"></span>
                    <span id="headerOperando">0</span> Operando
                </div>
                <div class="stat">
                    <span class="estado-dot dot-mantenimiento"></span>
                    <span id="headerMant">0</span> Mant.
                </div>
                <div class="stat">
                    <span class="estado-dot dot-alarma"></span>
                    <span id="headerAlarma">0</span> Alarmas
                </div>
                <div class="stat" style="color:#9ca3af;">
                    🕐 <span id="headerHora">--:--</span>
                </div>
            </div>
        </header>

        <div class="container">
            <div class="sidebar">
                <h2>📡 Plantas</h2>
                <ul class="plant-list" id="plantList"></ul>
                
                <div style="margin-top:20px; padding-top:15px; border-top:1px solid #374151;">
                    <h2>📊 Resumen Global</h2>
                    <div class="stats-grid" style="margin-top:10px;">
                        <div class="stat-item">
                            <div class="label">Pureza Prom</div>
                            <div class="value" id="globalPureza">--</div>
                        </div>
                        <div class="stat-item">
                            <div class="label">Flujo Total</div>
                            <div class="value" id="globalFlujo">--</div>
                        </div>
                    </div>
                </div>

                <div style="margin-top:20px; padding-top:15px; border-top:1px solid #374151;">
                    <h2>📥 Exportar</h2>
                    <div style="margin-top:10px;">
                        <button class="btn btn-success" style="width:100%;" onclick="exportarCSV()">
                            Descargar CSV
                        </button>
                    </div>
                </div>
            </div>

            <div class="content">
                <!-- Panel de controles -->
                <div class="controls-panel">
                    <div class="control-group">
                        <label>Planta:</label>
                        <strong id="plantaSeleccionadaLabel" style="color:#3b82f6;">--</strong>
                    </div>
                    <div class="control-group">
                        <label>Desde:</label>
                        <input type="datetime-local" id="filtroDesde">
                    </div>
                    <div class="control-group">
                        <label>Hasta:</label>
                        <input type="datetime-local" id="filtroHasta">
                    </div>
                    <button class="btn" onclick="cargarDatos()">Aplicar</button>
                    <button class="btn btn-secondary" onclick="setUltimas24h()">24h</button>
                    <button class="btn btn-secondary" onclick="setUltimos7d()">7 días</button>
                    <button class="btn btn-secondary" onclick="setUltimos30d()">30 días</button>
                    <div class="control-group" style="margin-left:auto;">
                        <label>Auto-refresh:</label>
                        <select id="autoRefresh" onchange="setAutoRefresh()">
                            <option value="0">Desactivado</option>
                            <option value="30">30 seg</option>
                            <option value="60" selected>1 min</option>
                            <option value="300">5 min</option>
                        </select>
                    </div>
                </div>

                <!-- Tarjeta de alarma (oculta por defecto) -->
                <div class="alarma-card" id="cardAlarma" style="display:none;">
                    <div class="icon">🚨</div>
                    <div class="info">
                        <h3>ALARMA ACTIVA</h3>
                        <p id="textoAlarma">--</p>
                    </div>
                </div>

                <!-- Tabs -->
                <div class="tabs">
                    <button class="tab active" onclick="cambiarTab('dashboard')">📊 Dashboard</button>
                    <button class="tab" onclick="cambiarTab('estadisticas')">📈 Estadísticas</button>
                    <button class="tab" onclick="cambiarTab('historial')">📋 Historial</button>
                </div>

                <!-- Tab Dashboard -->
                <div class="tab-content active" id="tab-dashboard">
                    <!-- Métricas principales -->
                    <div class="metrics-row">
                        <div class="metric-card">
                            <h3>🧪 Pureza O₂</h3>
                            <div class="valor" id="cardPureza">--</div>
                            <div class="subtexto" id="cardPurezaEstado">--</div>
                        </div>
                        <div class="metric-card">
                            <h3>💨 Flujo</h3>
                            <div class="valor" id="cardFlujo">--<span class="unidad">Nm³/h</span></div>
                            <div class="subtexto" id="cardFlujoTrend"></div>
                        </div>
                        <div class="metric-card">
                            <h3>📈 Presión</h3>
                            <div class="valor" id="cardPresion">--<span class="unidad">bar</span></div>
                            <div class="subtexto" id="cardPresionEstado">--</div>
                        </div>
                        <div class="metric-card">
                            <h3>🌡️ Temperatura</h3>
                            <div class="valor" id="cardTemp">--<span class="unidad">°C</span></div>
                            <div class="subtexto" id="cardTempEstado">--</div>
                        </div>
                        <div class="metric-card">
                            <h3>⚙️ Modo</h3>
                            <div class="valor" id="cardModo" style="font-size:20px;">--</div>
                            <div class="subtexto" id="cardHoras">--</div>
                        </div>
                    </div>

                    <!-- KPIs -->
                    <div class="kpi-row">
                        <div class="kpi-card">
                            <h4>Disponibilidad</h4>
                            <div class="kpi-value valor-ok" id="kpiDisponibilidad">--%</div>
                            <div class="kpi-bar">
                                <div class="kpi-bar-fill" id="kpiDispBar" style="background:#22c55e;width:0%"></div>
                            </div>
                        </div>
                        <div class="kpi-card">
                            <h4>Cumplimiento Pureza ≥93%</h4>
                            <div class="kpi-value" id="kpiPureza">--%</div>
                            <div class="kpi-bar">
                                <div class="kpi-bar-fill" id="kpiPurezaBar" style="background:#3b82f6;width:0%"></div>
                            </div>
                        </div>
                        <div class="kpi-card">
                            <h4>Registros en período</h4>
                            <div class="kpi-value" id="kpiRegistros" style="color:#9ca3af;">--</div>
                        </div>
                    </div>

                    <!-- Gráficos -->
                    <div class="charts-container">
                        <div class="chart-card">
                            <div class="chart-header">
                                <h3>🧪 Pureza O₂ (%)</h3>
                                <select id="chartTypePureza" onchange="actualizarTipoGrafico('pureza')">
                                    <option value="line">Línea</option>
                                    <option value="bar">Barras</option>
                                </select>
                            </div>
                            <canvas id="chartPureza"></canvas>
                        </div>
                        <div class="chart-card">
                            <div class="chart-header">
                                <h3>💨 Flujo (Nm³/h)</h3>
                                <select id="chartTypeFlujo" onchange="actualizarTipoGrafico('flujo')">
                                    <option value="line">Línea</option>
                                    <option value="bar">Barras</option>
                                </select>
                            </div>
                            <canvas id="chartFlujo"></canvas>
                        </div>
                        <div class="chart-card">
                            <div class="chart-header">
                                <h3>📈 Presión (bar)</h3>
                                <select id="chartTypePresion" onchange="actualizarTipoGrafico('presion')">
                                    <option value="line">Línea</option>
                                    <option value="bar">Barras</option>
                                </select>
                            </div>
                            <canvas id="chartPresion"></canvas>
                        </div>
                        <div class="chart-card">
                            <div class="chart-header">
                                <h3>🌡️ Temperatura (°C)</h3>
                                <select id="chartTypeTemp" onchange="actualizarTipoGrafico('temp')">
                                    <option value="line">Línea</option>
                                    <option value="bar">Barras</option>
                                </select>
                            </div>
                            <canvas id="chartTemp"></canvas>
                        </div>
                    </div>
                </div>

                <!-- Tab Estadísticas -->
                <div class="tab-content" id="tab-estadisticas">
                    <div class="stats-panel">
                        <h3>📊 Estadísticas del Período Seleccionado</h3>
                        <div style="display:grid; grid-template-columns:repeat(4, 1fr); gap:15px; margin-top:15px;">
                            <div>
                                <h4 style="color:#22c55e; margin-bottom:10px;">🧪 Pureza O₂ (%)</h4>
                                <div class="stats-grid">
                                    <div class="stat-item"><div class="label">Mínimo</div><div class="value" id="statPurezaMin">--</div></div>
                                    <div class="stat-item"><div class="label">Máximo</div><div class="value" id="statPurezaMax">--</div></div>
                                    <div class="stat-item"><div class="label">Promedio</div><div class="value" id="statPurezaAvg">--</div></div>
                                    <div class="stat-item"><div class="label">Desv. Est.</div><div class="value" id="statPurezaStd">--</div></div>
                                </div>
                            </div>
                            <div>
                                <h4 style="color:#3b82f6; margin-bottom:10px;">💨 Flujo (Nm³/h)</h4>
                                <div class="stats-grid">
                                    <div class="stat-item"><div class="label">Mínimo</div><div class="value" id="statFlujoMin">--</div></div>
                                    <div class="stat-item"><div class="label">Máximo</div><div class="value" id="statFlujoMax">--</div></div>
                                    <div class="stat-item"><div class="label">Promedio</div><div class="value" id="statFlujoAvg">--</div></div>
                                    <div class="stat-item"><div class="label">Desv. Est.</div><div class="value" id="statFlujoStd">--</div></div>
                                </div>
                            </div>
                            <div>
                                <h4 style="color:#eab308; margin-bottom:10px;">📈 Presión (bar)</h4>
                                <div class="stats-grid">
                                    <div class="stat-item"><div class="label">Mínimo</div><div class="value" id="statPresionMin">--</div></div>
                                    <div class="stat-item"><div class="label">Máximo</div><div class="value" id="statPresionMax">--</div></div>
                                    <div class="stat-item"><div class="label">Promedio</div><div class="value" id="statPresionAvg">--</div></div>
                                    <div class="stat-item"><div class="label">Desv. Est.</div><div class="value" id="statPresionStd">--</div></div>
                                </div>
                            </div>
                            <div>
                                <h4 style="color:#ef4444; margin-bottom:10px;">🌡️ Temperatura (°C)</h4>
                                <div class="stats-grid">
                                    <div class="stat-item"><div class="label">Mínimo</div><div class="value" id="statTempMin">--</div></div>
                                    <div class="stat-item"><div class="label">Máximo</div><div class="value" id="statTempMax">--</div></div>
                                    <div class="stat-item"><div class="label">Promedio</div><div class="value" id="statTempAvg">--</div></div>
                                    <div class="stat-item"><div class="label">Desv. Est.</div><div class="value" id="statTempStd">--</div></div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div class="charts-container" style="margin-top:15px;">
                        <div class="chart-card full-width">
                            <div class="chart-header">
                                <h3>📊 Distribución de Modos de Operación</h3>
                            </div>
                            <canvas id="chartModos" style="max-height:250px;"></canvas>
                        </div>
                    </div>
                </div>

                <!-- Tab Historial -->
                <div class="tab-content" id="tab-historial">
                    <div class="stats-panel">
                        <h3>📋 Historial de Datos</h3>
                        <div style="overflow-x:auto; margin-top:15px;">
                            <table style="width:100%; border-collapse:collapse; font-size:12px;">
                                <thead>
                                    <tr style="background:#1f2937;">
                                        <th style="padding:10px; text-align:left; border-bottom:1px solid #374151;">Fecha/Hora</th>
                                        <th style="padding:10px; text-align:center; border-bottom:1px solid #374151;">Pureza</th>
                                        <th style="padding:10px; text-align:center; border-bottom:1px solid #374151;">Flujo</th>
                                        <th style="padding:10px; text-align:center; border-bottom:1px solid #374151;">Presión</th>
                                        <th style="padding:10px; text-align:center; border-bottom:1px solid #374151;">Temp</th>
                                        <th style="padding:10px; text-align:center; border-bottom:1px solid #374151;">Modo</th>
                                        <th style="padding:10px; text-align:center; border-bottom:1px solid #374151;">Alarma</th>
                                    </tr>
                                </thead>
                                <tbody id="tablaHistorial">
                                    <tr><td colspan="7" style="text-align:center; padding:20px; color:#9ca3af;">Seleccione una planta y período</td></tr>
                                </tbody>
                            </table>
                        </div>
                        <div style="margin-top:15px; text-align:center;">
                            <span id="historialInfo" style="color:#9ca3af; font-size:12px;">Mostrando últimos 100 registros</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="footer">
            SCADA - Sistema de Control y Adquisición de Datos para Plantas PSA de Oxígeno | MSPyBS - DGGIES | v2.0
        </div>

        <script>
            const PLANTAS = """ + plantas_json + """;
            const API_KEY = '""" + API_KEY + """';

            let plantaSeleccionada = null;
            let datosActuales = [];
            let autoRefreshInterval = null;

            // Charts
            let chartPureza = null;
            let chartFlujo = null;
            let chartPresion = null;
            let chartTemp = null;
            let chartModos = null;

            // Configuración de colores para gráficos
            const chartColors = {
                pureza: { border: '#22c55e', bg: 'rgba(34, 197, 94, 0.1)' },
                flujo: { border: '#3b82f6', bg: 'rgba(59, 130, 246, 0.1)' },
                presion: { border: '#eab308', bg: 'rgba(234, 179, 8, 0.1)' },
                temp: { border: '#ef4444', bg: 'rgba(239, 68, 68, 0.1)' }
            };

            function estadoPlanta(planta) {
                if (planta.alarma) return 'alarma';
                if (planta.modo === 'Mantenimiento') return 'mantenimiento';
                return 'ok';
            }

            function actualizarHeader() {
                let operando = 0, mant = 0, alarma = 0;
                let purezaTotal = 0, flujoTotal = 0;

                Object.values(PLANTAS).forEach(p => {
                    if (p.alarma) alarma++;
                    else if (p.modo === 'Mantenimiento') mant++;
                    else operando++;
                    purezaTotal += p.pureza_pct || 0;
                    flujoTotal += p.flujo_nm3h || 0;
                });

                document.getElementById('headerOperando').textContent = operando;
                document.getElementById('headerMant').textContent = mant;
                document.getElementById('headerAlarma').textContent = alarma;
                document.getElementById('headerHora').textContent = new Date().toLocaleTimeString('es-PY', {hour:'2-digit', minute:'2-digit'});

                const numPlantas = Object.keys(PLANTAS).length;
                document.getElementById('globalPureza').textContent = numPlantas ? (purezaTotal / numPlantas).toFixed(1) + '%' : '--';
                document.getElementById('globalFlujo').textContent = flujoTotal.toFixed(1);
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
                    li.innerHTML = `
                        <span>${p.nombre || id}</span>
                        <div class="estado-dot dot-${estadoPlanta(p)}"></div>
                    `;
                    li.addEventListener('click', () => {
                        plantaSeleccionada = id;
                        crearListaPlantas();
                        cargarDatos();
                    });
                    lista.appendChild(li);
                });

                document.getElementById('plantaSeleccionadaLabel').textContent = 
                    plantaSeleccionada && PLANTAS[plantaSeleccionada] ? PLANTAS[plantaSeleccionada].nombre : '(ninguna)';
            }

            function cambiarTab(tabId) {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
                document.querySelector(`.tab[onclick*="${tabId}"]`).classList.add('active');
                document.getElementById('tab-' + tabId).classList.add('active');
            }

            function setUltimas24h() {
                const ahora = new Date();
                const hace24 = new Date(ahora.getTime() - 24 * 60 * 60 * 1000);
                document.getElementById('filtroDesde').value = formatDatetimeLocal(hace24);
                document.getElementById('filtroHasta').value = formatDatetimeLocal(ahora);
                cargarDatos();
            }

            function setUltimos7d() {
                const ahora = new Date();
                const hace7d = new Date(ahora.getTime() - 7 * 24 * 60 * 60 * 1000);
                document.getElementById('filtroDesde').value = formatDatetimeLocal(hace7d);
                document.getElementById('filtroHasta').value = formatDatetimeLocal(ahora);
                cargarDatos();
            }

            function setUltimos30d() {
                const ahora = new Date();
                const hace30d = new Date(ahora.getTime() - 30 * 24 * 60 * 60 * 1000);
                document.getElementById('filtroDesde').value = formatDatetimeLocal(hace30d);
                document.getElementById('filtroHasta').value = formatDatetimeLocal(ahora);
                cargarDatos();
            }

            function formatDatetimeLocal(d) {
                const pad = n => n.toString().padStart(2, '0');
                return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
            }

            function setAutoRefresh() {
                const interval = parseInt(document.getElementById('autoRefresh').value);
                if (autoRefreshInterval) {
                    clearInterval(autoRefreshInterval);
                    autoRefreshInterval = null;
                }
                if (interval > 0) {
                    autoRefreshInterval = setInterval(cargarDatos, interval * 1000);
                }
            }

            function actualizarTarjetas(ultimo) {
                if (!ultimo) {
                    document.getElementById('cardPureza').innerHTML = '--';
                    document.getElementById('cardFlujo').innerHTML = '--<span class="unidad">Nm³/h</span>';
                    document.getElementById('cardPresion').innerHTML = '--<span class="unidad">bar</span>';
                    document.getElementById('cardTemp').innerHTML = '--<span class="unidad">°C</span>';
                    document.getElementById('cardModo').textContent = '--';
                    document.getElementById('cardAlarma').style.display = 'none';
                    return;
                }

                const pureza = ultimo.pureza_pct ?? 0;
                const purezaClass = pureza >= 93 ? 'valor-ok' : pureza >= 90 ? 'valor-warning' : 'valor-danger';
                document.getElementById('cardPureza').innerHTML = `<span class="${purezaClass}">${pureza.toFixed(1)}%</span>`;
                document.getElementById('cardPurezaEstado').innerHTML = pureza >= 93 
                    ? '<span class="trend trend-up">✓ Apto uso médico</span>' 
                    : '<span class="trend trend-down">⚠ Bajo umbral</span>';

                document.getElementById('cardFlujo').innerHTML = `${(ultimo.flujo_nm3h ?? 0).toFixed(1)}<span class="unidad">Nm³/h</span>`;
                document.getElementById('cardPresion').innerHTML = `${(ultimo.presion_bar ?? 0).toFixed(1)}<span class="unidad">bar</span>`;
                document.getElementById('cardTemp').innerHTML = `${(ultimo.temperatura_c ?? 0).toFixed(1)}<span class="unidad">°C</span>`;

                const modo = ultimo.modo || 'Desconocido';
                const modoColor = modo === 'Producción' ? 'valor-ok' : modo === 'Mantenimiento' ? 'valor-warning' : '';
                document.getElementById('cardModo').innerHTML = `<span class="${modoColor}">${modo}</span>`;
                document.getElementById('cardHoras').textContent = `${(ultimo.horas_operacion || 0).toLocaleString()} horas op.`;

                if (ultimo.alarma) {
                    document.getElementById('cardAlarma').style.display = 'flex';
                    document.getElementById('textoAlarma').textContent = ultimo.mensaje_alarma || 'Alarma activa';
                } else {
                    document.getElementById('cardAlarma').style.display = 'none';
                }
            }

            function actualizarEstadisticas(datos) {
                if (!datos || datos.length === 0) {
                    ['Pureza', 'Flujo', 'Presion', 'Temp'].forEach(tipo => {
                        ['Min', 'Max', 'Avg', 'Std'].forEach(stat => {
                            document.getElementById(`stat${tipo}${stat}`).textContent = '--';
                        });
                    });
                    document.getElementById('kpiDisponibilidad').textContent = '--%';
                    document.getElementById('kpiPureza').textContent = '--%';
                    document.getElementById('kpiRegistros').textContent = '--';
                    return;
                }

                function calcStats(arr) {
                    const valid = arr.filter(v => v != null && !isNaN(v));
                    if (valid.length === 0) return { min: 0, max: 0, avg: 0, std: 0 };
                    const sum = valid.reduce((a, b) => a + b, 0);
                    const avg = sum / valid.length;
                    const variance = valid.reduce((acc, v) => acc + Math.pow(v - avg, 2), 0) / valid.length;
                    return {
                        min: Math.min(...valid),
                        max: Math.max(...valid),
                        avg: avg,
                        std: Math.sqrt(variance)
                    };
                }

                const statsPureza = calcStats(datos.map(d => d.pureza_pct));
                const statsFlujo = calcStats(datos.map(d => d.flujo_nm3h));
                const statsPresion = calcStats(datos.map(d => d.presion_bar));
                const statsTemp = calcStats(datos.map(d => d.temperatura_c));

                document.getElementById('statPurezaMin').textContent = statsPureza.min.toFixed(1);
                document.getElementById('statPurezaMax').textContent = statsPureza.max.toFixed(1);
                document.getElementById('statPurezaAvg').textContent = statsPureza.avg.toFixed(1);
                document.getElementById('statPurezaStd').textContent = '±' + statsPureza.std.toFixed(2);

                document.getElementById('statFlujoMin').textContent = statsFlujo.min.toFixed(1);
                document.getElementById('statFlujoMax').textContent = statsFlujo.max.toFixed(1);
                document.getElementById('statFlujoAvg').textContent = statsFlujo.avg.toFixed(1);
                document.getElementById('statFlujoStd').textContent = '±' + statsFlujo.std.toFixed(2);

                document.getElementById('statPresionMin').textContent = statsPresion.min.toFixed(1);
                document.getElementById('statPresionMax').textContent = statsPresion.max.toFixed(1);
                document.getElementById('statPresionAvg').textContent = statsPresion.avg.toFixed(1);
                document.getElementById('statPresionStd').textContent = '±' + statsPresion.std.toFixed(2);

                document.getElementById('statTempMin').textContent = statsTemp.min.toFixed(1);
                document.getElementById('statTempMax').textContent = statsTemp.max.toFixed(1);
                document.getElementById('statTempAvg').textContent = statsTemp.avg.toFixed(1);
                document.getElementById('statTempStd').textContent = '±' + statsTemp.std.toFixed(2);

                // KPIs
                const produccion = datos.filter(d => d.modo === 'Producción').length;
                const disponibilidad = (produccion / datos.length * 100);
                const purezaOk = datos.filter(d => (d.pureza_pct || 0) >= 93).length;
                const cumplimientoPureza = (purezaOk / datos.length * 100);

                document.getElementById('kpiDisponibilidad').textContent = disponibilidad.toFixed(1) + '%';
                document.getElementById('kpiDispBar').style.width = disponibilidad + '%';
                document.getElementById('kpiDisponibilidad').className = 'kpi-value ' + (disponibilidad >= 90 ? 'valor-ok' : disponibilidad >= 70 ? 'valor-warning' : 'valor-danger');

                document.getElementById('kpiPureza').textContent = cumplimientoPureza.toFixed(1) + '%';
                document.getElementById('kpiPurezaBar').style.width = cumplimientoPureza + '%';
                document.getElementById('kpiPureza').className = 'kpi-value ' + (cumplimientoPureza >= 95 ? 'valor-ok' : cumplimientoPureza >= 80 ? 'valor-warning' : 'valor-danger');

                document.getElementById('kpiRegistros').textContent = datos.length.toLocaleString();

                // Gráfico de modos
                const modos = {};
                datos.forEach(d => {
                    const m = d.modo || 'Desconocido';
                    modos[m] = (modos[m] || 0) + 1;
                });
                actualizarChartModos(modos);
            }

            function actualizarHistorial(datos) {
                const tbody = document.getElementById('tablaHistorial');
                
                if (!datos || datos.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; padding:20px; color:#9ca3af;">Sin datos para el período</td></tr>';
                    return;
                }

                // Mostrar últimos 100 registros en orden inverso
                const ultimos = datos.slice(-100).reverse();
                
                tbody.innerHTML = ultimos.map(d => `
                    <tr style="border-bottom:1px solid #1f2937;">
                        <td style="padding:8px;">${new Date(d.timestamp).toLocaleString('es-PY')}</td>
                        <td style="padding:8px; text-align:center;" class="${(d.pureza_pct || 0) >= 93 ? 'valor-ok' : 'valor-danger'}">${(d.pureza_pct || 0).toFixed(1)}%</td>
                        <td style="padding:8px; text-align:center;">${(d.flujo_nm3h || 0).toFixed(1)}</td>
                        <td style="padding:8px; text-align:center;">${(d.presion_bar || 0).toFixed(1)}</td>
                        <td style="padding:8px; text-align:center;">${(d.temperatura_c || 0).toFixed(1)}</td>
                        <td style="padding:8px; text-align:center;">${d.modo || '-'}</td>
                        <td style="padding:8px; text-align:center;">${d.alarma ? '🚨' : '✓'}</td>
                    </tr>
                `).join('');

                document.getElementById('historialInfo').textContent = `Mostrando ${ultimos.length} de ${datos.length} registros`;
            }

            function crearChart(ctx, label, color, tipo = 'line') {
                return new Chart(ctx, {
                    type: tipo,
                    data: {
                        labels: [],
                        datasets: [{
                            label: label,
                            data: [],
                            borderColor: color.border,
                            backgroundColor: tipo === 'line' ? color.bg : color.border,
                            borderWidth: 2,
                            tension: 0.3,
                            pointRadius: 0,
                            fill: tipo === 'line'
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { display: false }
                        },
                        scales: {
                            x: {
                                ticks: { color: '#9ca3af', maxTicksLimit: 8, font: { size: 10 } },
                                grid: { color: 'rgba(255,255,255,0.05)' }
                            },
                            y: {
                                ticks: { color: '#9ca3af', font: { size: 10 } },
                                grid: { color: 'rgba(255,255,255,0.05)' }
                            }
                        }
                    }
                });
            }

            function actualizarChart(chart, labels, data, tipo = null) {
                if (!chart) return;
                
                if (tipo && chart.config.type !== tipo) {
                    chart.config.type = tipo;
                    chart.data.datasets[0].fill = tipo === 'line';
                }
                
                chart.data.labels = labels;
                chart.data.datasets[0].data = data;
                chart.update('none');
            }

            function actualizarTipoGrafico(variable) {
                const tipo = document.getElementById(`chartType${variable.charAt(0).toUpperCase() + variable.slice(1)}`).value;
                
                const chartMap = {
                    'pureza': chartPureza,
                    'flujo': chartFlujo,
                    'presion': chartPresion,
                    'temp': chartTemp
                };

                if (chartMap[variable]) {
                    chartMap[variable].config.type = tipo;
                    chartMap[variable].data.datasets[0].fill = tipo === 'line';
                    chartMap[variable].update();
                }
            }

            function actualizarChartModos(modos) {
                const ctx = document.getElementById('chartModos');
                
                if (chartModos) {
                    chartModos.data.labels = Object.keys(modos);
                    chartModos.data.datasets[0].data = Object.values(modos);
                    chartModos.update();
                } else {
                    chartModos = new Chart(ctx, {
                        type: 'doughnut',
                        data: {
                            labels: Object.keys(modos),
                            datasets: [{
                                data: Object.values(modos),
                                backgroundColor: ['#22c55e', '#eab308', '#ef4444', '#9ca3af'],
                                borderWidth: 0
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: {
                                legend: {
                                    position: 'right',
                                    labels: { color: '#e5e7eb', font: { size: 12 } }
                                }
                            }
                        }
                    });
                }
            }

            function cargarDatos() {
                if (!plantaSeleccionada) return;

                const desde = document.getElementById('filtroDesde').value;
                const hasta = document.getElementById('filtroHasta').value;

                let url = `/api/historial_json?api_key=${encodeURIComponent(API_KEY)}&planta_id=${encodeURIComponent(plantaSeleccionada)}`;
                if (desde) url += `&desde=${encodeURIComponent(desde)}`;
                if (hasta) url += `&hasta=${encodeURIComponent(hasta)}`;

                fetch(url)
                    .then(r => r.json())
                    .then(datos => {
                        datosActuales = datos;

                        if (!Array.isArray(datos) || datos.length === 0) {
                            actualizarTarjetas(null);
                            actualizarEstadisticas([]);
                            actualizarHistorial([]);
                            
                            const labels = [];
                            actualizarChart(chartPureza, labels, []);
                            actualizarChart(chartFlujo, labels, []);
                            actualizarChart(chartPresion, labels, []);
                            actualizarChart(chartTemp, labels, []);
                            return;
                        }

                        // Actualizar tarjetas con último valor
                        actualizarTarjetas(datos[datos.length - 1]);

                        // Actualizar estadísticas
                        actualizarEstadisticas(datos);

                        // Actualizar historial
                        actualizarHistorial(datos);

                        // Preparar datos para gráficos
                        const labels = datos.map(d => {
                            const dt = new Date(d.timestamp);
                            return dt.toLocaleString('es-PY', {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'});
                        });

                        actualizarChart(chartPureza, labels, datos.map(d => d.pureza_pct ?? 0));
                        actualizarChart(chartFlujo, labels, datos.map(d => d.flujo_nm3h ?? 0));
                        actualizarChart(chartPresion, labels, datos.map(d => d.presion_bar ?? 0));
                        actualizarChart(chartTemp, labels, datos.map(d => d.temperatura_c ?? 0));
                    })
                    .catch(err => console.error('Error cargando datos:', err));
            }

            function exportarCSV() {
                if (!plantaSeleccionada) {
                    alert('Seleccione una planta primero');
                    return;
                }

                const desde = document.getElementById('filtroDesde').value;
                const hasta = document.getElementById('filtroHasta').value;

                let url = `/api/exportar_csv?api_key=${encodeURIComponent(API_KEY)}&planta_id=${encodeURIComponent(plantaSeleccionada)}`;
                if (desde) url += `&desde=${encodeURIComponent(desde)}`;
                if (hasta) url += `&hasta=${encodeURIComponent(hasta)}`;

                window.location.href = url;
            }

            // Inicialización
            document.addEventListener('DOMContentLoaded', () => {
                // Crear charts
                chartPureza = crearChart(document.getElementById('chartPureza').getContext('2d'), 'Pureza', chartColors.pureza);
                chartFlujo = crearChart(document.getElementById('chartFlujo').getContext('2d'), 'Flujo', chartColors.flujo);
                chartPresion = crearChart(document.getElementById('chartPresion').getContext('2d'), 'Presión', chartColors.presion);
                chartTemp = crearChart(document.getElementById('chartTemp').getContext('2d'), 'Temperatura', chartColors.temp);

                // Configurar filtros por defecto (últimas 24h)
                setUltimas24h();

                // Crear lista de plantas
                crearListaPlantas();

                // Actualizar header
                actualizarHeader();
                setInterval(actualizarHeader, 30000);

                // Configurar auto-refresh
                setAutoRefresh();
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
    """Ejecuta Flask en modo producción."""
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)


def main():
    global sistema_alertas, USUARIOS
    
    print("=" * 60)
    print("  BOT PLANTAS O2 PSA - SCADA v2.0")
    print("  Dashboard profesional con estadísticas avanzadas")
    print("=" * 60)
    
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
    print(f"✓ API Key: {API_KEY[:8]}...")
    
    # Inicializar DB
    inicializar_db()
    print("✓ Base de datos inicializada")
    
    # Cargar usuarios
    USUARIOS = cargar_usuarios()
    print(f"✓ Usuarios cargados: {len(USUARIOS.get('admins', []))} admins")
    
    # Google Sheets
    if inicializar_google_sheets():
        print("✓ Google Sheets conectado")
    else:
        print("○ Google Sheets no configurado")
    
    # Telegram App
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Sistema de alertas
    sistema_alertas = SistemaAlertas(app)
    print("✓ Sistema de alertas inicializado")
    
    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("mi_id", mi_id))
    app.add_handler(CommandHandler("ayuda", ayuda))
    app.add_handler(CommandHandler("help", ayuda))
    app.add_handler(CommandHandler("stats", estadisticas_cmd))
    app.add_handler(CommandHandler("estadisticas", estadisticas_cmd))
    app.add_handler(CommandHandler("nueva_planta", nueva_planta))
    app.add_handler(CommandHandler("eliminar_planta", eliminar_planta))
    app.add_handler(CommandHandler("config_alertas", config_alertas))
    app.add_handler(CommandHandler("config_umbrales", config_umbrales))
    app.add_handler(CommandHandler("exportar", exportar_cmd))
    app.add_handler(CommandHandler("agregar_admin", agregar_admin))
    app.add_handler(CommandHandler("agregar_operador", agregar_operador))
    app.add_handler(CommandHandler("agregar_lector", agregar_lector))
    app.add_handler(CommandHandler("remover_usuario", remover_usuario))
    app.add_handler(CommandHandler("listar_usuarios", listar_usuarios))
    app.add_handler(CallbackQueryHandler(manejar_callback))
    
    print("✓ Handlers de Telegram registrados")
    
    # Iniciar Flask en hilo separado
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print(f"✓ API REST iniciada en puerto {PORT}")
    print(f"✓ Dashboard disponible en: /dashboard?api_key={API_KEY}")
    
    print("=" * 60)
    print("Bot iniciado correctamente!")
    print("=" * 60)
    
    # Iniciar bot
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
