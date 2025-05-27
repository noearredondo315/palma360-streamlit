import streamlit as st
import pandas as pd
import time
from utils.supabase_client import SupabaseClient


class DataLoader:
    """Centralized data loader for StreamlPT application"""
    _instance = None
    _tables_loaded = {}
    _data_frames = {}
    _unique_values = {}
    
    def __new__(cls, table_name=None, default_columns=None):
        if cls._instance is None:
            cls._instance = super(DataLoader, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, table_name=None, default_columns=None):
        """Initialize data loader
        
        Args:
            table_name: The name of the table to query (can be set later)
            default_columns: Default columns to retrieve (if None, all columns will be retrieved)
        """
        if self._initialized:
            return
            
        self.table_name = table_name
        self.default_columns = default_columns
        self.supabase_client = None
        self.sql_agent = None
        self._initialized = True
    
    def initialize_clients(self):
        """Initialize Supabase client and SQL agent"""
        try:
            # Initialize Supabase client
            self.supabase_client = SupabaseClient()
            
            # Initialize SQL agent - usando versión simple
            try:
                openai_api_key = st.secrets["openai"]["OPENAI_API_KEY"]
                # Ensure table_name is set before initializing SQL agent
                if not self.table_name:
                    raise ValueError("Table name must be set before initializing SQL agent")
                # self.sql_agent = SQLAgent(table_name=self.table_name, openai_api_key=openai_api_key) 
            except Exception as e:
                # st.error(f"Error initializing SQL agent: {e}") 
                self.sql_agent = None
                
        except Exception as e:
            st.error(f"Error initializing Supabase client: {e}")
            return False
    
    def load_all_data(self, progress_callback=None, table_name=None, columns=None):
        """Load all necessary data with optional progress callback
        
        Args:
            progress_callback: Optional callback function for progress updates
            table_name: Table name to load (can be a single table name or a list of table names)
            columns: Optional columns to retrieve (can be a dict with table names as keys)
            
        Returns:
            bool: True if all data was loaded successfully, False otherwise
        """
        try:
            # Handle table_name as either a single string or a list of strings
            table_names = [table_name] if isinstance(table_name, str) else (table_name or [self.table_name])
            
            # Initialize clients if not already done
            if not self.supabase_client and not self.initialize_clients():
                return False
            
            # Track progress for each table
            total_tables = len(table_names)
            loaded_tables = 0
            
            for i, current_table in enumerate(table_names):
                # Skip if table is already loaded
                if current_table in self._tables_loaded:
                    loaded_tables += 1
                    continue
                
                # Calculate progress
                progress = loaded_tables / total_tables
                
                # Update progress
                if progress_callback:
                    progress_callback(progress, f"Cargando tabla {current_table}...")
                
                # Get columns for this table
                table_columns = None
                if isinstance(columns, dict):
                    table_columns = columns.get(current_table)
                
                # Load table data
                df = self.supabase_client.get_table_data(
                    table_name=current_table,
                    columns=table_columns,
                    default_columns=self.default_columns
                )
                
                if df is None or df.empty:
                    st.warning(f"No se encontraron datos para la tabla {current_table}")
                    continue
                
                # Store the loaded dataframe
                self._data_frames[current_table] = df
                
                # Mark table as loaded
                self._tables_loaded[current_table] = True
                loaded_tables += 1
                
                # Small delay for better UX
                time.sleep(0.2)
            
            # Final progress update
            if progress_callback:
                progress_callback(1.0, "¡Datos cargados completamente!")
                
            return True
                
        except Exception as e:
            error_msg = f"Error cargando datos: {str(e)}"
            if progress_callback:
                progress_callback(1.0, error_msg)
            st.error(error_msg)
            return False
    
    def get_dataframe(self):
        """Get the loaded dataframe
        
        Returns:
            pd.DataFrame: The loaded dataframe or None if not found
        """
        # Return the first available dataframe
        if self._data_frames:
            return next(iter(self._data_frames.values()))
        return None
    
    def get_unique_values(self, column_name):
        """Get unique values for a specific column"""
        # If we already have precalculated values, return them
        if column_name in self.unique_values:
            return self.unique_values[column_name]
            
        # Otherwise calculate on demand
        if self.df is not None and not self.df.empty and column_name in self.df.columns:
            values = self.df[column_name].dropna().unique()
            return sorted([str(v) for v in values])
            
        return []
    
    def get_data(self):
        """Get the main dataframe"""
        return self.df

    def get_dataframe(self):
        """Compatibilidad: Devuelve el DataFrame principal o vacío si no está cargado."""
        if self.df is not None and not self.df.empty:
            return self.df
        else:
            return pd.DataFrame()
        
    def get_sql_agent(self):
        """Get the SQL agent"""
        return self.sql_agent
