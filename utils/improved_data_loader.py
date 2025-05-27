import streamlit as st
import pandas as pd
import concurrent.futures
import threading
from utils.supabase_client import SupabaseClient # Ensure this is the non-singleton version
from utils.config import get_config

class ImprovedDataLoader:
    _instance = None
    _singleton_creation_lock = threading.RLock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._singleton_creation_lock:
                if cls._instance is None:
                    cls._instance = super(ImprovedDataLoader, cls).__new__(cls)
                    cls._instance._initialized_loader_state = False
        return cls._instance

    def __init__(self, supabase_url=None, supabase_key=None):
        if hasattr(self, '_initialized_loader_state') and self._initialized_loader_state:
            return

        if not supabase_url or not supabase_key:
            raise ValueError("Supabase URL and Key are required for ImprovedDataLoader first initialization via factory.")

        with self._singleton_creation_lock: # Protects __init__ attributes during first init
            if hasattr(self, '_initialized_loader_state') and self._initialized_loader_state:
                return
            
            self.supabase_url = supabase_url
            self.supabase_key = supabase_key
            
            self._data_frames = {}
            self._tables_loaded_status = {} # Stores True/False based on load success
            self._data_access_lock = threading.RLock() # For _data_frames and _tables_loaded_status
            self._unique_values = {} # For caching unique column values
            
            self.default_table_name = get_config("KIOSKO_VISTA")
            self.sql_agent = None # Placeholder if needed later

            self._initialized_loader_state = True

    def clear_cache(self):
        """Clears cached dataframes and their loaded statuses."""
        with self._data_access_lock:
            self._data_frames.clear()
            self._tables_loaded_status.clear()
            self._unique_values.clear() # Also clear cached unique values if any
        # Optionally, log this action or provide feedback if run in a context where that's useful
        # For now, just clearing silently as it's typically part of a reload process.

    def _load_single_table(self, table_name, columns, shared_progress, total_tables, progress_queue, progress_lock):
        message_for_ui = ""
        local_supabase_client = None
        try:
            # Instantiate a new SupabaseClient for this thread
            local_supabase_client = SupabaseClient(self.supabase_url, self.supabase_key)

            with self._data_access_lock:
                if self._tables_loaded_status.get(table_name) and self._data_frames.get(table_name) is not None and not self._data_frames.get(table_name).empty:
                    with progress_lock:
                        shared_progress["loaded_tables"] += 1
                    progress = shared_progress["loaded_tables"] / total_tables # Read can be outside lock
                    if progress_queue:
                        progress_queue.put({"progress": progress, "message": f"Tabla {table_name} ya estaba cargada.", "status_type": "info", "table_name": table_name, "source": "_load_single_table_already_loaded"})
                    return table_name, True, self._data_frames.get(table_name), f"Tabla {table_name} ya estaba cargada."

            df = local_supabase_client.get_table_data(table_name=table_name, columns=columns)
            
            success = False
            if df is not None and not df.empty:
                with self._data_access_lock:
                    self._data_frames[table_name] = df
                    self._tables_loaded_status[table_name] = True
                with progress_lock:
                    shared_progress["loaded_tables"] += 1
                progress = shared_progress["loaded_tables"] / total_tables
                if progress_queue:
                    progress_queue.put({"progress": progress, "message": f"Cargando tabla {table_name}...", "status_type": "info", "table_name": table_name, "source": "_load_single_table_loading"})
                success = True
                message_for_ui = f"Datos para la tabla {table_name} cargados correctamente."
                return table_name, success, df, message_for_ui
            else:
                with self._data_access_lock:
                    self._tables_loaded_status[table_name] = False 
                with progress_lock:
                    shared_progress["loaded_tables"] += 1
                progress = shared_progress["loaded_tables"] / total_tables
                if progress_queue:
                    progress_queue.put({"progress": progress, "message": f"No se encontraron datos para {table_name} o la tabla está vacía.", "status_type": "warning", "table_name": table_name, "source": "_load_single_table_no_data"})
                message_for_ui = f"No se encontraron datos para la tabla {table_name} o la tabla está vacía."
                return table_name, False, None, message_for_ui
        except Exception as e:
            exception_type = type(e).__name__
            details_parts = [f"str(e): {str(e) if str(e) else 'N/A'}"]
            for attr in ['message', 'details', 'hint', 'code']:
                if hasattr(e, attr):
                    val = getattr(e, attr)
                    details_parts.append(f"e.{attr}: {val if val is not None else 'N/A'}")
            
            exception_attributes_str = ", ".join(details_parts)
            error_origin = "_load_single_table"
            error_msg = (
                f"Origen: {error_origin}, Tabla: '{table_name}', "
                f"Tipo: [{exception_type}], Atributos: [{exception_attributes_str}]"
            )
            
            with self._data_access_lock:
                self._tables_loaded_status[table_name] = False
            with progress_lock:
                shared_progress["loaded_tables"] += 1
                if error_msg not in shared_progress["errors"]:
                    shared_progress["errors"].append(error_msg)
            progress = shared_progress["loaded_tables"] / total_tables
            if progress_queue:
                progress_queue.put({"progress": progress, "message": f"Error al cargar {table_name}.", "status_type": "error", "table_name": table_name, "source": "_load_single_table_error"})
            return table_name, False, None, error_msg
            
    def load_specific_tables(self, tables_config, progress_queue=None):
        detailed_messages_for_ui = []
        if not tables_config:
            if progress_queue: progress_queue.put({"progress": 1.0, "message": "No hay tablas especificadas para cargar.", "status_type": "info", "table_name": "N/A", "source": "load_specific_tables_no_tables"})
            return True, [("info", "N/A", "No hay tablas especificadas para cargar.")]

        total_tables = len(tables_config)
        shared_progress = {"loaded_tables": 0, "errors": []}
        progress_lock = threading.Lock() # Lock for shared_progress updates

        if progress_queue: progress_queue.put({"progress": 0.0, "message": "Inicializando carga de datos...", "status_type": "info", "table_name": "System", "source": "load_specific_tables_initializing"})

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(total_tables, 4)) as executor:
            futures = {}
            for table_name, columns in tables_config.items():
                with self._data_access_lock:
                    if self._tables_loaded_status.get(table_name) and self._data_frames.get(table_name) is not None and not self._data_frames.get(table_name).empty:
                        with progress_lock:
                            shared_progress["loaded_tables"] += 1
                        progress = shared_progress["loaded_tables"] / total_tables
                        msg_content = f"Tabla {table_name} ya estaba cargada."
                        detailed_messages_for_ui.append(("info", table_name, msg_content))
                        if progress_queue: progress_queue.put({"progress": progress, "message": msg_content, "status_type": "info", "table_name": table_name, "source": "load_specific_tables_already_loaded"})
                        continue
                
                future = executor.submit(
                    self._load_single_table,
                    table_name, columns, shared_progress, total_tables, progress_queue, progress_lock
                )
                futures[future] = table_name

            for future in concurrent.futures.as_completed(futures):
                table_name_from_future = futures[future]
                try:
                    returned_table_name, success_status, _df, message_content = future.result()
                    status_type = "success" if success_status else ("warning" if "No se encontraron datos" in message_content else "error")
                    detailed_messages_for_ui.append((status_type, returned_table_name, message_content))
                except Exception as e:
                    exception_type = type(e).__name__
                    details_parts = [f"str(e): {str(e) if str(e) else 'N/A'}"]
                    for attr in ['message', 'details', 'hint', 'code']:
                        if hasattr(e, attr):
                            val = getattr(e, attr)
                            details_parts.append(f"e.{attr}: {val if val is not None else 'N/A'}")
                    exception_attributes_str = ", ".join(details_parts)
                    error_origin = "load_specific_tables.future_error"
                    err_msg_content = (
                        f"Origen: {error_origin}, Tarea para: '{table_name_from_future}', "
                        f"Tipo: [{exception_type}], Atributos: [{exception_attributes_str}]"
                    )
                    detailed_messages_for_ui.append(("error", table_name_from_future, err_msg_content))
                    with progress_lock:
                        if err_msg_content not in shared_progress["errors"]:
                           shared_progress["errors"].append(err_msg_content)
            
        overall_success = not any(status == "error" for status, _, _ in detailed_messages_for_ui)
        if progress_queue: # Changed from progress_callback
            status_message = "."
            if not overall_success:
                status_message = " con errores."
            elif any(s == "warning" for s, _, _ in detailed_messages_for_ui):
                status_message = " con advertencias."
            final_msg = "Carga de datos completada" + status_message
            overall_status_type = "success"
            if "errores" in status_message:
                overall_status_type = "error"
            elif "advertencias" in status_message:
                overall_status_type = "warning"
            # The inner 'if progress_queue:' was redundant as the outer one now serves this purpose.
            progress_queue.put({"progress": 1.0, "message": final_msg, "status_type": overall_status_type, "table_name": "Overall", "source": "load_specific_tables_completed"})
        return overall_success, detailed_messages_for_ui

    def load_all_required_tables(self, progress_queue=None):
        kiosko_name = get_config("KIOSKO_VISTA")
        contabilidad_name = get_config("CONTABILIDAD")
        desglosado_name = get_config("DESGLOSADO")
        concentrado_name = get_config("CONCENTRADO")

        tables_to_load_config = {
            kiosko_name: get_config("KIOSKO_VISTA_COLUMNS"),
            contabilidad_name: None,
            desglosado_name: get_config("DEFAULT_COLUMNS"),
            concentrado_name: get_config("CONSULTA")
        }
        return self.load_specific_tables(tables_to_load_config, progress_queue)

    def get_dataframe(self, table_key=None):
        """
        Retrieves a dataframe from the loaded data.

        Args:
            table_key (Optional[str]): Optional key to specify which table to retrieve.
                                       Can be 'kiosko', 'contabilidad', or 'desglosado'.

        Returns:
            Union[Tuple[pd.DataFrame, pd.DataFrame], pd.DataFrame]: 
                If table_key is None: Tuple of (df_concentrado, df_desglosado) 
                                      for backwards compatibility.
                If table_key is specified: The specified dataframe.
                If data is not available: Empty dataframe(s).
        """
        config = get_config()
        
        # If no key is specified, return tuple for backwards compatibility
        if table_key is None:
            # This part is for maintaining compatibility with previous versions
            # that expected a tuple. Consider refactoring call sites eventually.
            df_contabilidad = self._data_frames.get(config.get("CONTABILIDAD"), pd.DataFrame())
            df_desglosado = self._data_frames.get(config.get("DESGLOSADO"), pd.DataFrame())
            return df_contabilidad, df_desglosado
            
        # Return specific dataframe based on key
        # Ensure table_key is a string before calling .lower()
        if isinstance(table_key, str) and table_key.lower() == 'kiosko':
            return self._data_frames.get(config.get("KIOSKO_VISTA"), pd.DataFrame())
        elif table_key.lower() == 'contabilidad':
            return self._data_frames.get(config.get("CONTABILIDAD"), pd.DataFrame())
        elif table_key.lower() == 'desglosado':
            return self._data_frames.get(config.get("DESGLOSADO"), pd.DataFrame())
        elif table_key.lower() == 'consulta':
            return self._data_frames.get(config.get("CONCENTRADO"), pd.DataFrame())
        else:
            st.warning(f"Tabla '{table_key}' no reconocida")
            return pd.DataFrame()
    
    def get_kiosko_dataframe(self):
        """Get the kiosko dataframe for visualization page"""
        config = get_config()
        return self._data_frames.get(config.get("KIOSKO_VISTA"), pd.DataFrame())
    
    def get_contabilidad_dataframe(self):
        """Get the contabilidad dataframe for Base_Datos tab2"""
        config = get_config()
        return self._data_frames.get(config.get("CONTABILIDAD"), pd.DataFrame())
    
    def get_desglosado_dataframe(self):
        """Get the desglosado dataframe for Base_Datos tab1"""
        config = get_config()
        return self._data_frames.get(config.get("DESGLOSADO"), pd.DataFrame())
    
    def get_consulta_dataframe(self):
        """Get the consulta dataframe for Base_Datos tab2"""
        config = get_config()
        return self._data_frames.get(config.get("CONCENTRADO"), pd.DataFrame())
    
    def get_all_tables_loaded(self):
        """Check if all required tables have been loaded"""
        config = get_config()
        required_tables = [
            config.get("KIOSKO_VISTA"),
            config.get("CONTABILIDAD"),
            config.get("DESGLOSADO"),
            config.get("CONCENTRADO")
        ]
        
        return all(self._tables_loaded_status.get(table, False) for table in required_tables)
    
    def get_unique_values(self, table_key, column_name):
        """Get unique values for a specific column in a specific table"""
        # If we already have precalculated values, return them
        key = f"{table_key}_{column_name}"
        if key in self._unique_values:
            return self._unique_values[key]
            
        # Otherwise calculate on demand
        df = self.get_dataframe(table_key)
        if isinstance(df, tuple):
            df = df[0]  # Use first dataframe in tuple as default
        
        if df is not None and not df.empty and column_name in df.columns:
            values = df[column_name].dropna().unique()
            self._unique_values[key] = sorted([str(v) for v in values])
            return self._unique_values[key]
            
        return []
    
    def get_sql_agent(self):
        """Get the SQL agent"""
        return self.sql_agent


# Global instance accessor function
@st.cache_resource
def get_improved_data_loader():
    """Get the global improved data loader instance
    
    Returns:
        ImprovedDataLoader: Singleton instance
    """
    supabase_url = st.secrets["supabase"]["url"]
    supabase_key = st.secrets["supabase"]["key"]
    return ImprovedDataLoader(supabase_url=supabase_url, supabase_key=supabase_key)
