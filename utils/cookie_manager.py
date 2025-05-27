import streamlit as st
from streamlit_cookies_controller import CookieController
import json

class CookieManager:
    """Manages authentication cookies for Streamlit application"""
    
    def __init__(self):
        """Initialize the cookie controller"""
        self.controller = CookieController()
        self.cookie_name = 'streaml_pt_auth'
        self.cookie_expiry = 7  # Days until cookie expires
    
    def save_auth_to_cookie(self, auth_data):
        """Save authentication data to cookie
        
        Args:
            auth_data (dict): Authentication data to save
        """
        # We'll store: authenticated status, user_id, username, name (display name), and email
        # We won't store the token in cookies for security reasons
        auth_cookie = {
            'authenticated': auth_data.get('authenticated', False),
            'user_id': auth_data.get('user_id'),
            'username': auth_data.get('username'),
            'name': auth_data.get('name'),
            'email': auth_data.get('email')
        }
        
        # Ensure cookie data is correctly formatted
        try:
            # Set cookie with expiration - streamlit-cookies-controller expects a string
            self.controller.set(
                self.cookie_name, 
                auth_cookie,  # The controller will handle serialization
                {'path': '/', 'maxAge': 60*60*24*self.cookie_expiry}  # Expire after specified days
            )
        except Exception as e:
            st.error(f"Error al guardar cookie: {e}")
            # Fallback: try with JSON serialization
            try:
                self.controller.set(
                    self.cookie_name, 
                    json.dumps(auth_cookie),
                    {'path': '/', 'maxAge': 60*60*24*self.cookie_expiry}
                )
            except Exception as e:
                st.error(f"Error al guardar cookie (fallback): {e}")
    
    def load_auth_from_cookie(self):
        """Load authentication data from cookie
        
        Returns:
            dict: Authentication data or None if cookie doesn't exist
        """
        auth_cookie = self.controller.get(self.cookie_name)
        
        if auth_cookie:
            # Check if the cookie is already a dictionary (streamlit-cookies-controller may return parsed JSON)
            if isinstance(auth_cookie, dict):
                return auth_cookie
            # Otherwise, try to parse it as JSON
            try:
                return json.loads(auth_cookie)
            except (json.JSONDecodeError, TypeError):
                # Invalid cookie format
                self.clear_auth_cookie()
                return None
        return None
    
    def restore_session_from_cookie(self):
        """Restore session state from cookie if available
        
        Returns:
            bool: True if session was restored, False otherwise
        """
        auth_data = self.load_auth_from_cookie()
        
        if auth_data and auth_data.get('authenticated'):
            # Restore session state variables
            st.session_state['authenticated'] = True
            st.session_state['user_id'] = auth_data.get('user_id')
            st.session_state['username'] = auth_data.get('username')
            st.session_state['name'] = auth_data.get('name')
            st.session_state['email'] = auth_data.get('email')
            return True
        return False
    
    def clear_auth_cookie(self):
        """Remove authentication cookie"""
        self.controller.remove(self.cookie_name, {'path': '/'})
    
    def get_all_cookies(self):
        """Get all cookies for debugging"""
        return self.controller.getAll()
