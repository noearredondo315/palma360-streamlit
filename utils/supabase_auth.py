import streamlit as st
from supabase import create_client
import json

class SupabaseAuth:
    def __init__(self):
        self.url = st.secrets["supabase"]["url"]
        self.key = st.secrets["supabase"]["key"]
        self.client = create_client(self.url, self.key)
        
        # Verificar si tenemos clave de admin para gestión avanzada de usuarios
        self.has_admin_access = "admin_key" in st.secrets.get("supabase", {})
        if self.has_admin_access:
            self.admin_key = st.secrets["supabase"]["admin_key"]
            self.admin_client = create_client(self.url, self.admin_key)

    def sign_in(self, credential, password):
        """Inicia sesión con email o username y devuelve información adicional del usuario"""
        try:
            # Primero intentamos iniciar sesión directamente con el email
            response = self.client.auth.sign_in_with_password({
                "email": credential, 
                "password": password
            })
            
            if response and response.user:
                # Extraer metadatos del usuario
                user_metadata = response.user.user_metadata or {}
                return {
                    "auth_response": response,
                    "display_name": user_metadata.get("display_name", response.user.email.split('@')[0]),
                    "username": user_metadata.get("username", response.user.email),
                    "email": response.user.email,
                    "user_id": response.user.id
                }
            return None
        except Exception as e:
            # Si no funciona como email, podríamos buscar por username si tenemos acceso admin
            if self.has_admin_access:
                return self._sign_in_with_username(credential, password)
            print(f"Error de autenticación: {str(e)}")
            return None
    
    def _sign_in_with_username(self, username, password):
        """Método auxiliar para iniciar sesión con username en lugar de email"""
        try:
            # Listar usuarios para encontrar el email correspondiente al username
            users_response = self.admin_client.auth.admin.list_users()
            
            # La respuesta ya es una lista directa de usuarios, no necesitamos extraer
            users = users_response
            
            # Imprimir para depuración
            print(f"Usuarios encontrados: {len(users)}")
            
            # Buscar usuario por username o email en metadatos
            for user in users:
                # Obtener metadatos y propiedades de forma segura
                user_email = getattr(user, 'email', None)
                user_metadata = getattr(user, 'user_metadata', {}) or {}
                user_username = user_metadata.get("username")
                
                print(f"Verificando usuario: {user_email}, username: {user_username}")
                
                # Verificar si coincide el username o email
                if username == user_username or username == user_email:
                    print(f"Usuario encontrado: {user_email}")
                    # Usar el email para autenticar
                    response = self.client.auth.sign_in_with_password({
                        "email": user_email,
                        "password": password
                    })
                    
                    if response and response.user:
                        display_name = user_metadata.get("display_name", user_email.split('@')[0])
                        print(f"Login exitoso. Display name: {display_name}, Username: {user_username}")
                        return {
                            "auth_response": response,
                            "display_name": display_name,
                            "username": user_username or user_email,
                            "email": user_email,
                            "user_id": response.user.id
                        }
            return None
        except Exception as e:
            print(f"Error al iniciar sesión con username: {str(e)}")
            return None

    def sign_up(self, email, password, username=None, display_name=None):
        """Registra un nuevo usuario con metadatos opcionales"""
        user_metadata = {}
        if username:
            user_metadata["username"] = username
        if display_name:
            user_metadata["display_name"] = display_name
            
        return self.client.auth.sign_up({
            "email": email, 
            "password": password,
            "options": {"data": user_metadata}
        })

    def sign_out(self):
        return self.client.auth.sign_out()

    def get_user(self):
        return self.client.auth.get_user()
        
    def get_user_profile(self, user_id):
        """Obtener perfil de usuario desde la base de datos (si existe tabla de perfiles)"""
        try:
            response = self.client.table('profiles').select('*').eq('id', user_id).execute()
            if response and hasattr(response, 'data') and len(response.data) > 0:
                return response.data[0]
            return None
        except Exception as e:
            print(f"Error obteniendo perfil: {str(e)}")
            return None
