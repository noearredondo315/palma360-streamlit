import streamlit as st
import os
import pandas as pd
import numpy as np
from utils.authentication import Authentication
# from utils.data_loader import get_data_loader


# Configure the page - debe ser el primer comando de Streamlit
st.set_page_config(
    page_title="Palma Terra 360",
    page_icon="assets/logoPT360.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load custom CSS
def load_css():
    with open(os.path.join("assets", "styles.css")) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

try:
    load_css()
except Exception as e:
    st.warning(f"Unable to load custom CSS: {e}")

# Initialize authentication
authentication = Authentication()

# Verificar si el usuario está autenticado
def check_user_auth():
    # Si no está autenticado y no está en la página de login,
    # mostrar formulario de login
    if not st.session_state.get('authenticated', False):
        authentication_status, name, username = authentication.login()
        if not authentication_status:
            st.stop()

# Función para definir la estructura de navegación según el estado de carga
def get_navigation_structure():
    # Verificar si los datos están cargados completamente
    data_loaded = st.session_state.get("data_fully_loaded", False)
    
    # Definir las páginas base (siempre disponibles)
    navigation_structure = {
        "Dashboard": [
            st.Page("pages/0_Dashboard.py", title="Dashboard"),
        ]
    }
    
    # Si los datos están cargados, agregar las herramientas
    if data_loaded:
        navigation_structure["Herramientas"] = [
            st.Page("pages/1_SQL_Chatbot.py", title="Chat interactivo"),
            st.Page("pages/3_Visualizacion_Datos.py", title="Visualización de datos"),
            st.Page("pages/4_Base_Datos.py", title="Base de datos"),
        ]
    # Si no están cargados, la sección "Herramientas" simplemente no existirá en el diccionario
    
    # Retornar la estructura de navegación construida
    return navigation_structure

# La función show_main_dashboard() se eliminó y su lógica se incorporó directamente en main()

# Ocultar sidebar completamente cuando no está autenticado
def hide_sidebar():
    st.markdown("""
    <style>
    [data-testid="collapsedControl"] {
        display: none !important;
    }
    section[data-testid='stSidebar'] {
        display: none !important;
    }
    </style>
    """, unsafe_allow_html=True)

# Constante para identificar qué página se está mostrando actualmente
# Está garantizado que siempre será "none" para la página principal
MAIN_PAGE_ID = "none"

# Guarda en una variable de contexto global (os.environ) el ID de la página actual
# Esto es más fiable que session_state porque es accesible desde cualquier proceso de Python
def set_page_type():
    import os
    current_page = st.query_params.get("page", MAIN_PAGE_ID)
    
    # Guarda el identificador de página actual en variables de entorno
    # para que sea accesible desde cualquier parte de la aplicación
    os.environ['STREAMLIT_CURRENT_PAGE'] = current_page
    
    # También lo guardamos en session_state para mayor compatibilidad
    st.session_state['current_page'] = current_page
    
    # Marcamos explícitamente si estamos en la página principal
    is_main_page = current_page == MAIN_PAGE_ID
    st.session_state['is_main_page'] = is_main_page
    return is_main_page

# Main application logic
def main():
    # Verificar si el usuario está autenticado
    if not authentication.check_authentication():
        # Si no está autenticado, ocultar sidebar completamente y mostrar solo el formulario de login
        hide_sidebar()
        authentication.login()
    else:
        # Configurar el tipo de página (principal o secundaria)
        is_main_page = set_page_type()
        
        # Obtener la estructura de navegación actualizada
        navigation_structure = get_navigation_structure()
        
        # Mostrar la navegación en todos los casos
        pg = st.navigation(navigation_structure)
        pg.run()
        

            


# Run the app
if __name__ == "__main__":
    main()
