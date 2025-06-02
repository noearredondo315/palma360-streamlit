import streamlit as st
from supabase import create_client, Client
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests
import os

# ------------------------ CONFIGURACIÓN ------------------------

# Constantes para Google Cloud
PROJECT_ID = "seraphic-jet-458916-u0"
ZONE = "us-central1-a"
INSTANCE_NAME = "training-pt360"
TRIGGER_URL = "https://us-central1-seraphic-jet-458916-u0.cloudfunctions.net/start-vm"

# Ruta temporal para las credenciales
LOCAL_CREDENTIALS_PATH = "/tmp/service_account.json"

# ------------------------ FUNCIONES ------------------------

@st.cache_resource
def init_supabase_client():
    """Inicializa y devuelve un cliente de Supabase"""
    url = st.secrets["supabase"]["url"]
    key = st.secrets["supabase"]["key"]
    return create_client(url, key)

@st.cache_resource
def load_credentials_from_supabase(bucket_name="startupvm", file_name="seraphic-jet-458916-u0-41b09484a682.json") -> str:
    """Carga las credenciales de Google Cloud desde Supabase Storage"""
    supabase = init_supabase_client()
    file_response = supabase.storage.from_(bucket_name).download(file_name)
    with open(LOCAL_CREDENTIALS_PATH, "wb") as f:
        f.write(file_response)
    return LOCAL_CREDENTIALS_PATH

def get_vm_status(credentials_path: str) -> str:
    """Obtiene el estado actual de la VM"""
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    service = build("compute", "v1", credentials=credentials)
    result = service.instances().get(
        project=PROJECT_ID,
        zone=ZONE,
        instance=INSTANCE_NAME
    ).execute()
    return result["status"]

def trigger_vm_startup() -> dict:
    """Envía una solicitud para iniciar la VM y devuelve el resultado"""
    try:
        response = requests.get(TRIGGER_URL)
        success = response.ok
        return {
            "success": success,
            "status_code": response.status_code,
            "message": "Petición enviada correctamente." if success else f"Error al llamar la función. Código: {response.status_code}"
        }
    except Exception as e:
        return {
            "success": False,
            "status_code": None,
            "message": f"Error: {str(e)}"
        }

def render_vm_control_button(authorized_username="l-gutierrez"):
    """Renderiza el botón de control de VM solo si el usuario está autorizado"""
    # Verificar si el usuario está autenticado y es el usuario autorizado (por defecto l-gutierrez)
    is_authorized = (st.session_state.get('authenticated', False) and 
                    st.session_state.get('username') == authorized_username)
    
    if not is_authorized:
        return
    
    st.markdown("---")
    st.subheader("🖥️ Actualizar Base de datos")
    
    try:
        credentials_path = load_credentials_from_supabase()
        vm_status = get_vm_status(credentials_path)

        if vm_status == "RUNNING":
            st.success("✅ La actualización está actualmente en ejecución.")
            st.info("ℹ️ Tiempo aproximado de ejecución: 7~8 minutos.")
        else:            
            # Inicializar el estado de confirmación si no existe
            if 'confirmar_ejecucion' not in st.session_state:
                st.session_state.confirmar_ejecucion = False
                
            # Botón inicial para ejecutar el flujo
            if not st.session_state.confirmar_ejecucion:
                if st.button("🔵 Actualizar servidor"):
                    st.session_state.confirmar_ejecucion = True
                    st.rerun()
            
            # Mostrar confirmación si se ha pulsado el botón inicial
            if st.session_state.confirmar_ejecucion:
                st.warning("¿Estás seguro de que deseas iniciar la actualización? Esta acción puede tardar unos minutos. ⚠️", icon="⚠️")
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("✅ Sí, iniciar actualización"):
                        result = trigger_vm_startup()
                        if result["success"]:
                            st.success(f"✅ {result['message']}")
                        else:
                            st.error(f"❌ {result['message']}")
                        st.session_state.confirmar_ejecucion = False
                
                with col2:
                    if st.button("❌ Cancelar"):
                        st.session_state.confirmar_ejecucion = False
                        st.rerun()
    except Exception as e:
        st.error(f"❌ Error al verificar el estado de la actualización: {str(e)}")
