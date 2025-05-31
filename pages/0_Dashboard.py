import streamlit as st
import time
import pandas as pd
import queue
import numpy as np
from datetime import datetime
import threading
from streamlit.runtime.scriptrunner import add_script_run_ctx

from utils.authentication import Authentication
from utils.config import get_config
from utils.improved_data_loader import get_improved_data_loader, ImprovedDataLoader # Ensure ImprovedDataLoader is importable for type hinting or direct use if needed
from utils.loading_dialog import loading_data_dialog # Import the refactored dialog
from supabase import create_client, Client
from collections import Counter

# Initialize Authentication
authentication = Authentication()

# --- Session State Initialization for Data Loading and Dialog ---
def initialize_session_state_for_loading():
    default_states = {
        "dialog_is_open": False,
        "dialog_progress_value": 0.0,
        "dialog_progress_message": "Iniciando...",
        "dialog_detailed_messages": [],
        "dialog_loading_finished": False,
        "dialog_overall_success": True,
        "dialog_loader_thread_active": False,
        "data_loaded_once": False, # Tracks if data has been successfully loaded at least once
        "trigger_initial_load": False # This can be set by authentication.py upon new login
    }
    for key, value in default_states.items():
        if key not in st.session_state:
            st.session_state[key] = value

initialize_session_state_for_loading()

# --- Authentication Check --- 
# Needs to happen after session state init if auth relies on it, but typically standalone.
if not authentication.check_authentication():
    authentication.login() # This might set st.session_state.trigger_initial_load = True
    st.stop()

# --- Supabase Configuration ---
supabase_url = st.secrets["supabase"]["url"]
supabase_key = st.secrets["supabase"]["key"]
if not supabase_url or not supabase_key:
    st.error("Configuración de Supabase no encontrada. No se pueden cargar datos.")
    st.stop()

# progress_callback_for_ui is removed as updates will be handled via queue

def _execute_data_loading():
    """Target function for the data loading thread."""
    try:
        # Simular progreso para que el usuario sepa que la aplicación está inicializando
        # En este nuevo enfoque, no cargamos datos globalmente, cada página lo hace por su cuenta
        
        # Emitir mensajes de progreso a través de la cola
        progress_steps = [
            {"progress": 0.2, "message": "Inicializando aplicación...", "status_type": "info", "table_name": "Sistema"},
            {"progress": 0.5, "message": "Preparando la interfaz...", "status_type": "info", "table_name": "Sistema"},
            {"progress": 0.8, "message": "Configurando conexiones...", "status_type": "info", "table_name": "Sistema"},
            {"progress": 1.0, "message": "¡Listo! Aplicación inicializada correctamente", "status_type": "success", "table_name": "Sistema"}
        ]
        
        for step in progress_steps:
            st.session_state.progress_queue.put(step)
            time.sleep(0.5)  # Simular tiempo de carga
            
        # Marcar como exitoso
        st.session_state.dialog_overall_success = True
        st.session_state.data_loaded_once = True
        st.session_state.data_fully_loaded = True  # Agregar esta bandera para habilitar la navegación
        st.session_state.data_load_timestamp = datetime.now()
    except Exception as e:
        st.session_state.dialog_overall_success = False
        st.session_state.dialog_detailed_messages.append({
            "type": "error", "table": "Sistema", "message": f"Error al inicializar la aplicación: {str(e)}"
        })
    finally:
        st.session_state.dialog_progress_value = 1.0  # Ensure bar is full
        st.session_state.dialog_loading_finished = True
        st.session_state.dialog_loader_thread_active = False

def start_threaded_data_load(clear_cache=False):
    """Inicia el proceso de inicialización de la aplicación en un hilo separado."""
    if st.session_state.get('dialog_loader_thread_active', False):
        st.toast("La inicialización ya está en progreso.", icon="⏳")
        return

    if clear_cache:
        # Ya no usamos data_loader.clear_cache()
        # Simplemente reiniciamos los estados de sesión relevantes
        st.session_state.data_loaded_once = False

    # Reset dialog state for a new loading operation
    st.session_state.dialog_is_open = True
    st.session_state.dialog_progress_value = 0.0
    st.session_state.dialog_progress_message = "Iniciando la aplicación..."
    st.session_state.dialog_detailed_messages = []
    st.session_state.dialog_loading_finished = False
    st.session_state.dialog_overall_success = True # Assume success until failure
    st.session_state.dialog_loader_thread_active = True

    # Initialize the progress queue in session state
    st.session_state.progress_queue = queue.Queue()

    # Create and start the thread, ensuring Streamlit context
    thread = threading.Thread(target=_execute_data_loading)
    add_script_run_ctx(thread)
    thread.start()
    st.rerun() # Immediately rerun to show the dialog and start its update cycle

# --- Sidebar ---
# Definimos algunos estilos personalizados para el sidebar
sidebar_css = """
<style>
    .sidebar-header {
        display: flex;
        align-items: center;
        gap: 10px;
        margin-bottom: 20px;
    }
    .sidebar-header img {
        border-radius: 8px;
        box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
    }
    .logo-title {
        color: #2C3E50;
        font-size: 24px;
        font-weight: 600;
        margin: 0;
    }
    .welcome-container {
        background-color: #EBF5FB;
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 20px;
        border-left: 4px solid #3498DB;
    }
    .reload-button {
        margin-top: 15px;
        margin-bottom: 20px;
    }
    .date-display {
        font-size: 14px;
        color: #7F8C8D;
        margin-top: 5px;
    }
    .custom-divider {
        height: 2px;
        background: linear-gradient(to right, #3498DB, transparent);
        margin: 20px 0;
        border: none;
    }
    .logout-btn {
        display: inline-flex;
        align-items: center;
        background-color: #E74C3C;
        color: white;
        padding: 8px 16px;
        border-radius: 5px;
        text-decoration: none;
        font-weight: 500;
        transition: all 0.3s ease;
        border: none;
        cursor: pointer;
    }
    .logout-btn:hover {
        background-color: #C0392B;
        box-shadow: 0 2px 5px rgba(0, 0, 0, 0.2);
    }
    .logout-icon {
        margin-right: 8px;
    }
</style>
"""

# Obtenemos la fecha y hora actual en español
from datetime import datetime
import locale

try:
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')  # Para sistemas Unix/Linux/Mac
except:
    try:
        locale.setlocale(locale.LC_TIME, 'es-ES')  # Para Windows
    except:
        pass  # Si no se puede configurar el locale en español, usamos el predeterminado

fecha_actual = datetime.now().strftime("%A, %d de %B de %Y")
fecha_actual = fecha_actual.capitalize()

# Iconos modernos en SVG
dashboard_icon = """<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#3498DB" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="9"/><rect x="14" y="3" width="7" height="5"/><rect x="14" y="12" width="7" height="9"/><rect x="3" y="16" width="7" height="5"/></svg>"""

user_icon = """<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#3498DB" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path><circle cx="12" cy="7" r="4"></circle></svg>"""

logout_icon = """<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"></path><polyline points="16 17 21 12 16 7"></polyline><line x1="21" y1="12" x2="9" y2="12"></line></svg>"""

calendar_icon = """<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#7F8C8D" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"></rect><line x1="16" y1="2" x2="16" y2="6"></line><line x1="8" y1="2" x2="8" y2="6"></line><line x1="3" y1="10" x2="21" y2="10"></line></svg>"""

# Renderizamos el sidebar con los nuevos estilos
with st.sidebar:
    st.markdown(sidebar_css, unsafe_allow_html=True)
    
    # Encabezado con logo y título
    st.markdown(f"""
    <div class="sidebar-header">
        {dashboard_icon}
        <h1 class="logo-title">StreamlPT</h1>
    </div>
    """, unsafe_allow_html=True)
    
    # Contenedor de bienvenida con nombre de usuario y fecha
    st.markdown(f"""
    <div class="welcome-container">
        <div style="color: #2C3E50; font-weight: 500;">{user_icon} <b>Bienvenido, {st.session_state.get('name', 'Usuario')}!</b></div>
        <div class="date-display" style="color: #34495E;">{calendar_icon} {fecha_actual}</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Botón para recargar datos
    reload_icon = """<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M23 4v6h-6"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>"""
    
    st.markdown("<div class='reload-button'></div>", unsafe_allow_html=True)
    
    if st.button("Recargar datos", use_container_width=True, type="primary"):
        start_threaded_data_load(clear_cache=True)
        # The st.rerun() inside start_threaded_data_load will handle UI update
    
    # Divisor personalizado
    st.markdown('<hr class="custom-divider">', unsafe_allow_html=True)
    
    # El botón de logout ahora se maneja completamente desde authentication.py
    
    # Evitamos duplicar el botón de logout al haber sido mejorado en authentication.py
    authentication.logout()

# --- Main Application Logic ---
# This section determines what to display based on loading state, dialog state, etc.

# 1. Check if initial data load needs to be triggered (e.g., after login)
if st.session_state.get('trigger_initial_load', False) and \
   not st.session_state.get('dialog_loader_thread_active', False) and \
   not st.session_state.get('data_loaded_once', False):
    st.session_state.trigger_initial_load = False # Reset trigger
    start_threaded_data_load()

# 2. If dialog is open, display it and process queue.
if st.session_state.get('dialog_is_open', False):
    loading_data_dialog() 

    if st.session_state.get('dialog_loader_thread_active', False):
        try:
            while not st.session_state.progress_queue.empty(): # Process all messages in queue
                item = st.session_state.progress_queue.get_nowait()
                st.session_state.dialog_progress_value = item.get("progress", st.session_state.dialog_progress_value)
                st.session_state.dialog_progress_message = item.get("message", st.session_state.dialog_progress_message)
                
                if not isinstance(st.session_state.get('dialog_detailed_messages'), list):
                    st.session_state.dialog_detailed_messages = []
                
                st.session_state.dialog_detailed_messages.append({
                    "type": item.get("status_type", "info"),
                    "table": item.get("table_name", "N/A"),
                    "message": item.get("message", "")
                })
        except queue.Empty:
            pass # No new messages
        
        # If the thread is still active (meaning loading is ongoing), schedule a rerun to check queue again.
        if st.session_state.get('dialog_loader_thread_active', False):
            time.sleep(0.2) # Polling frequency
            st.rerun()
    # If the dialog is open, but the loader thread is NOT active AND loading is marked as finished:
    elif not st.session_state.get('dialog_loader_thread_active', False) and \
         st.session_state.get('dialog_loading_finished', False):
        # Loading thread is done, and loading process is marked as finished.
        # Close the dialog automatically to allow user interaction with the app.
        st.session_state.dialog_is_open = False
        st.rerun() # Rerun to hide dialog and show main content/updated state

# 3. If dialog is NOT open:
elif not st.session_state.get('data_loaded_once', False) and \
     not st.session_state.get('dialog_loader_thread_active', False):
    # Data not loaded, no thread active, dialog not open.
    # This implies an initial state or a state after a failed load where dialog was closed.
    # Attempt to load data. start_threaded_data_load() will open the dialog.
    start_threaded_data_load()

elif st.session_state.get('data_loaded_once', False):
    # Data is loaded, and dialog is not open (due to the elif structure).
    # Show the main dashboard content (MOVED FROM PREVIOUS ELSE BLOCK).
    main_container = st.container()
    with main_container:
        # Un estilo minimalista para las tarjetas de métricas
        st.markdown("""
        <style>
        .simple-card {
            background-color: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12);
            text-align: center;
            height: 100%;
        }
        .card-value {
            font-size: 28px;
            font-weight: 600;
            margin: 10px 0;
            color: #1C84E4;
        }
        .card-label {
            font-size: 16px;
            color: #555;
            margin-bottom: 5px;
        }
        .card-delta-positive {
            color: #34D399;
            font-size: 14px;
        }
        .card-delta-negative {
            color: #F87171;
            font-size: 14px;
        }
        </style>
        """, unsafe_allow_html=True)

    # Títulos principales del dashboard
    st.title("Panel Factuas | Palma Terra 360")
    
    # Usamos un estilo personalizado para el subheader con color oscuro fijo
    st.markdown(f"""
    <h3 style="color: #2C3E50; font-weight: 500; margin-bottom: 0.5rem;">
        Bienvenido, {st.session_state.get('name', 'Usuario')}
    </h3>
    """, unsafe_allow_html=True)
    
    st.info(""":bar_chart: **Portal de Proveedores Palma Terra 360**

Bienvenido al centro integral de análisis Palma Terra. Este panel proporciona métricas clave e información actualizada sobre las facturas y proyectos. 

Explore las siguientes secciones para aprovechar al máximo la plataforma:

- **:speech_balloon: Chat interactivo**: Consulte su base de datos usando lenguaje natural para obtener respuestas específicas e información detallada
- **:chart_with_upwards_trend: Visualización de Datos**: Analice tendencias y patrones mediante gráficos interactivos y filtros personalizables""")
    
    st.markdown("<br>", unsafe_allow_html=True)  # Espacio adicional
    
    # Helper function to format dates
    def format_date_to_spanish(date_value):
        if isinstance(date_value, str):
            try:
                date_value = pd.to_datetime(date_value)
            except:
                return str(date_value)
        
        if isinstance(date_value, pd.Timestamp):
            meses_es = {
                1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
                5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
                9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
            }
            mes_es = meses_es.get(date_value.month, f"Mes {date_value.month}")
            # Formatear hora en formato 12 horas (AM/PM)
            hora = date_value.hour
            minuto = date_value.minute
            am_pm = "pm" if hora >= 12 else "am"
            hora_12 = hora % 12
            if hora_12 == 0:
                hora_12 = 12
            
            return f"{date_value.day} de {mes_es} {date_value.year} {hora_12:02d}:{minuto:02d} {am_pm}"
        else:
            return str(date_value)

    # Obtener los datos para las métricas
    # Initialize metrics with default values
    obras_count = 0
    total_facturado_fmt = "$0.00"
    ultima_actualizacion_fmt = "No disponible"
    total_conceptos = 0
    ultimo_registro = "No disponible"
    top_concepto = "No disponible"
    nombres = "No disponible"

    # Inicializar cliente Supabase directamente con las variables ya obtenidas
    try:
        supabase = create_client(supabase_url, supabase_key)
        
        # 1. Cantidad de obras únicas
        obras_response = supabase.table("portal_desglosado").select("obra").execute()
        obras_count = obras_response.data
        unique_obras = list({item["obra"] for item in obras_count if "obra" in item})
        obras_count = len(unique_obras)

        # 2. Total facturado
        total_facturado_response = supabase.table("portal_desglosado").select("subtotal").execute()
        total_facturado_data = total_facturado_response.data
        total_facturado = sum(item.get('subtotal', 0) for item in total_facturado_data)
        total_facturado_fmt = f"${total_facturado:,.2f}" if total_facturado else "$0.00"

        # 3. Última actualización de datos (basado en la fecha más reciente de 'fecha_factura')
        latest_date_response = supabase.table("portal_desglosado").select("fecha_factura").order("fecha_factura", desc=True).limit(1).maybe_single().execute()
        latest_date_data = latest_date_response.data
        if latest_date_data and latest_date_data.get("fecha_factura"):
            ultima_actualizacion_fmt = format_date_to_spanish(pd.to_datetime(latest_date_data["fecha_factura"]))
        else:
            ultima_actualizacion_fmt = "No disponible"
        
        # 4. Total de conceptos únicos
        total_conceptos = len(obras_response.data)  # Total number of rows returned

        # 5. Último registro (concepto más reciente)
        latest_date_response = supabase.table("portal_concentrado").select("fecha_consulta").order("fecha_consulta", desc=True).limit(1).maybe_single().execute()
        latest_date_data = latest_date_response.data
        if latest_date_data and latest_date_data.get("fecha_consulta"):
            ultimo_registro = format_date_to_spanish(pd.to_datetime(latest_date_data["fecha_consulta"]))
        else:
            ultimo_registro = "No disponible"

        # 6. Concepto más facturado (Top 1)
        subcategoria_response = supabase.table("portal_desglosado").select("subcategoria").execute()
        subcategoria_data = subcategoria_response.data

        # Extract subcategoria values into a list
        subcategorias = [item["subcategoria"] for item in subcategoria_data if item.get("subcategoria")]

        # Count frequencies and get the top 2 most common
        if subcategorias:
            subcategoria_counts = Counter(subcategorias)
            top_2_subcategorias = subcategoria_counts.most_common(2)
            # Extraer los nombres de las subcategorías
            nombres = [item[0] for item in top_2_subcategorias]
            # Unir los nombres en una cadena con ' - '
            nombres = ' - '.join(nombres)
        else:
            nombres = "No disponible"

    except Exception as e:
        st.error(f"Error al calcular métricas del dashboard desde Supabase: {e}")
    
    # Primera fila de métricas
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class="simple-card">
            <div class="card-label">Total de Obras</div>
            <div class="card-value">{obras_count}</div>
            <div class="card-delta-positive">Proyectos Registrados</div>
        </div>
        """, unsafe_allow_html=True)
        
    
    with col2:
        st.markdown(f"""
        <div class="simple-card">
            <div class="card-label">Subtotal Facturado ($)</div>
            <div class="card-value">{total_facturado_fmt}</div>
            <div class="card-delta-positive">Facturado a la fecha</div>
        </div>
        """, unsafe_allow_html=True)
        
    with col3:
        st.markdown(f"""
        <div class="simple-card">
            <div class="card-label">Fecha de última factura</div>
            <div class="card-value">{ultima_actualizacion_fmt}</div>
            <div class="card-delta-positive">Fecha más reciente</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)  # Espacio adicional
    
    # Segunda fila de métricas
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class="simple-card">
            <div class="card-label">Total Conceptos</div>
            <div class="card-value">{total_conceptos:,}</div>
            <div class="card-delta-positive">Registros totales</div>
        </div>
        """, unsafe_allow_html=True)
        
    with col2:
        st.markdown(f"""
        <div class="simple-card">
            <div class="card-label">Concepto Más Facturado</div>
            <div class="card-value">{nombres}</div>
            <div class="card-delta-positive">Partidas más facturadas</div>
        </div>
        """, unsafe_allow_html=True)
        
    with col3:
        st.markdown(f"""
        <div class="simple-card">
            <div class="card-label">Actualizado a la fecha</div>
            <div class="card-value">{ultimo_registro}</div>
            <div class="card-delta-positive">Fecha de captura</div>
        </div>
        """, unsafe_allow_html=True)

else:
    # Fallback for any other unexpected state.
    st.info("Cargando aplicación o esperando acción...")
    time.sleep(0.1)
    st.rerun()
