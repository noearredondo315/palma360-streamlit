import streamlit as st
import pandas as pd
from utils.config import get_config

# --- Funciones centralizadas para acceso a datos ---

def get_data_loader_instance(table_name=None, default_columns=None, load_data=True):
    """
    Obtiene una instancia configurada del cargador de datos mejorado
    
    Args:
        table_name: (Compatibilidad) Nombre de la tabla a cargar si se usa un método específico
        default_columns: (Compatibilidad) Columnas predeterminadas
        load_data: Si es True, verifica que todas las tablas necesarias estén cargadas
        
    Returns:
        Instancia configurada de ImprovedDataLoader
    """
    # Importar el nuevo cargador de datos mejorado
    from utils.improved_data_loader import get_improved_data_loader
    
    # Obtener la instancia del cargador de datos mejorado
    data_loader = get_improved_data_loader()
    
    # Verificar si es necesario cargar todos los datos
    if load_data and not data_loader.get_all_tables_loaded():
        # Cargar todas las tablas necesarias para la aplicación
        data_loader.load_all_required_tables()
    
    return data_loader


def get_column_mapping():
    """
    Obtiene el mapeo de columnas para visualización
    
    Returns:
        Diccionario con el mapeo de columnas (nombre en BD -> nombre para mostrar)
    """
    return get_config('COLUMN_MAPPING')
