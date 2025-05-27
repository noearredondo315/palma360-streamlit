import streamlit as st
from utils.supabase_auth import SupabaseAuth
from utils.cookie_manager import CookieManager
# Removed: from utils.loading_dialog import trigger_loading_dialog

class Authentication:
    """Simple authentication implementation for Streamlit"""
    
    def __init__(self):
        self.supabase_auth = SupabaseAuth()
        self.cookie_manager = CookieManager()
        
        # Initialize session state variables if they don't exist
        if 'authenticated' not in st.session_state:
            st.session_state['authenticated'] = False
        if 'name' not in st.session_state:
            st.session_state['name'] = None
        if 'username' not in st.session_state:
            st.session_state['username'] = None
        if 'user_token' not in st.session_state:
            st.session_state['user_token'] = None
            
        # Try to restore session from cookie if user isn't authenticated yet
        if not st.session_state['authenticated']:
            self.cookie_manager.restore_session_from_cookie()
    

    
    def login(self):
        """Display login form and handle authentication via Supabase"""
        authentication_status = False
        name = None
        username = None

        if not st.session_state.get('authenticated', False):
            login_col1, login_col2, login_col3 = st.columns([1, 2, 1])
            with login_col2:
                st.markdown("""
                <style>
                div[data-testid="column"]:nth-of-type(2) {
                    background-color: #ffffff;
                    border-radius: 10px;
                    padding: 20px;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                }
                </style>
                """, unsafe_allow_html=True)
                st.markdown("<h1 style='text-align: center; margin-bottom: 20px;'>✨ Iniciar Sesión</h1>", unsafe_allow_html=True)
                st.markdown("<p style='text-align: center; margin-bottom: 30px;'>Ingresa tus credenciales para acceder a la aplicación</p>", unsafe_allow_html=True)

                with st.form(key="login_form_auth"):
                    email = st.text_input("📧 Email", key="login_email_auth")
                    password = st.text_input("🔑 Contraseña", type="password", key="login_password_auth")
                    col1_form, col2_form, col3_form = st.columns([1, 2, 1])
                    with col2_form:
                        login_button_submitted = st.form_submit_button("🚀 Iniciar Sesión", use_container_width=True)

                import re
                def is_valid_credential(credential):
                    # Considera válido si es email o username (al menos 3 caracteres)
                    return bool(credential) and len(credential) >= 3

                if login_button_submitted and email and password:
                    if not is_valid_credential(email):
                        st.warning("⚠️ Ingresa un email o username válido.")
                    else:
                        try:
                            # Usar el nuevo método sign_in que acepta email o username
                            auth_result = self.supabase_auth.sign_in(email, password)
                            if auth_result and 'auth_response' in auth_result:
                                # Guardar la información básica de autenticación
                                st.session_state['authenticated'] = True
                                st.session_state['user_id'] = auth_result['user_id']
                                st.session_state['email'] = auth_result['email']
                                
                                # Guardar nombre de usuario y nombre para mostrar
                                st.session_state['username'] = auth_result['username']
                                st.session_state['name'] = auth_result['display_name']
                                
                                # Guardar token de sesión
                                response = auth_result['auth_response']
                                st.session_state['user_token'] = response.session.access_token if response.session else None
                                
                                # Establecer variables para retorno
                                authentication_status = True
                                name = auth_result['display_name']
                                username = auth_result['username']
                                
                                # Save authentication data to cookie for persistence
                                self.cookie_manager.save_auth_to_cookie(st.session_state)
                                
                                # Signal 0_Dashboard.py to trigger data loading
                                st.session_state.trigger_initial_load = True
                                # Ensure data_loaded_once is False so dashboard attempts load, 
                                # and dialog sequence is correctly initiated by 0_Dashboard.py
                                st.session_state.data_loaded_once = False 
                                if 'dialog_is_open' in st.session_state: # Reset any lingering dialog state from previous sessions
                                    st.session_state.dialog_is_open = False
                                if 'dialog_loader_thread_active' in st.session_state:
                                    st.session_state.dialog_loader_thread_active = False
                                
                                st.success(f"✅ Bienvenido, {name}! Preparando el sistema...")
                                st.rerun()
                            else:
                                st.warning("❌ Usuario o contraseña inválidos o cuenta no confirmada.")
                        except Exception as e:
                            st.warning(f"Error de autenticación, email/contraseña inválidos: {str(e)}")
                elif login_button_submitted:
                    st.warning("⚠️ Por favor ingresa email o username y contraseña.")
        else:
            authentication_status = True
            name = st.session_state.get('name')
            username = st.session_state.get('username')
        return authentication_status, name, username

    def logout(self):
        """Log out the current user from Supabase and redirect to login page"""
        import sys
        # Definimos el estilo CSS personalizado para el botón de logout
        logout_icon = """<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"></path><polyline points="16 17 21 12 16 7"></polyline><line x1="21" y1="12" x2="9" y2="12"></line></svg>"""
        
        # Estilo CSS para personalizar los botones de Streamlit - Color azul más oscuro (#2471A3)
        st.markdown("""
        <style>
            /* Estilo para el botón de logout */
            div[data-testid="stButton"] > button {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                background-color: #2471A3; /* Azul más oscuro */
                color: white;
                border-radius: 5px;
                font-weight: 500;
                transition: all 0.3s ease;
                border: none;
                box-shadow: none;
            }
            div[data-testid="stButton"] > button:hover {
                background-color: #1A5276; /* Tono aún más oscuro para hover */
                box-shadow: 0 2px 5px rgba(0, 0, 0, 0.2);
            }
            div[data-testid="stButton"] > button:active {
                background-color: #1A5276;
                border-color: transparent;
            }
            div[data-testid="stButton"] > button:focus {
                box-shadow: none;
                border-color: transparent;
            }
            /* Estilo para el contenedor del icono y botón */
            .icon-container {
                display: flex;
                align-items: center;
                justify-content: center;
                padding-top: 5px;
            }
        </style>
        """, unsafe_allow_html=True)
        
        # Aplicamos estilos al contenedor del sidebar
        st.sidebar.markdown("""
        <style>
            .css-1oe6wy4 {padding-top: 1rem !important;}
        </style>
        """, unsafe_allow_html=True)
        
        # Usamos un único botón con emoji en lugar de SVG
        if st.sidebar.button("⇠ Cerrar Sesión", key="logout_button", use_container_width=True):
            self.supabase_auth.sign_out()
            # Clear session state
            st.session_state['authenticated'] = False
            st.session_state['name'] = None
            st.session_state['username'] = None
            st.session_state['user_token'] = None
            
            # Clear authentication cookie
            self.cookie_manager.clear_auth_cookie()
            
            st.rerun()
            st.session_state["redirect_to_home"] = False
        script_path = sys.argv[0]
        is_in_pages = '/pages/' in script_path
        if is_in_pages:
            st.info("ℹ️ You have been logged out. Redirecting to login page...")
            st.markdown("<meta http-equiv='refresh' content='1; url=/'>", unsafe_allow_html=True)
            st.stop()
    
    def is_authenticated(self):
        """Check if user is authenticated"""
        return st.session_state.get('authenticated', False)
    
    def check_authentication(self):
        """Check authentication status and display appropriate message"""
        # Inicializar la variable si no existe
        if 'authenticated' not in st.session_state:
            st.session_state['authenticated'] = False
        
        # Verificar si el usuario está autenticado
        if not st.session_state.get('authenticated', False):
            # Mostrar el formulario de login
            return False
        
        return True
