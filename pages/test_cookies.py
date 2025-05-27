import streamlit as st
from utils.cookie_manager import CookieManager

st.title("Cookie Test Page")

# Create an instance of the cookie manager
cookie_manager = CookieManager()

# Display all cookies for debugging
st.subheader("Cookies actuales")
st.json(cookie_manager.get_all_cookies())

# Show authentication status
st.subheader("Estado de autenticación")
st.write(f"Autenticado: {st.session_state.get('authenticated', False)}")
st.write(f"Usuario: {st.session_state.get('username', 'No autenticado')}")
st.write(f"Nombre: {st.session_state.get('name', 'No autenticado')}")
