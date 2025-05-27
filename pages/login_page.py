import streamlit as st
from utils.authentication import Authentication

# No usar st.set_page_config aquu00ed ya que causa conflictos

# Inicializar autenticaciu00f3n
authentication = Authentication()

# Mostrar formulario de inicio de sesiu00f3n
authentication.login()
