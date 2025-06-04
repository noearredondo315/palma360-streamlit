import streamlit as st
import pandas as pd
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
    is_string_dtype, # Import added
)
from typing import List, Optional, Dict, Any

def custom_dataframe_explorer(df: pd.DataFrame, explorer_id: str, case: bool = True, multiselect_columns: Optional[List[str]] = None, fecha_columns: Optional[List[str]] = None, numeric_columns: Optional[List[str]] = None, text_columns: Optional[List[str]] = None, excluded_filter_columns: Optional[List[str]] = None, container=None) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns, with customized
    filtering options for specific text columns. Uses st.session_state to persist filters.
    
    Args:
        df (pd.DataFrame): The source dataframe to filter.
        explorer_id (str): Unique ID for this explorer to persist filters in session state.
        case (bool, optional): Whether to respect case when filtering. Defaults to True.
        multiselect_columns (List[str], optional): Columns to force multiselect UI. Defaults to None.
        fecha_columns (List[str], optional): Date columns that should be processed as dates. Defaults to None.
        numeric_columns (List[str], optional): Columns that should be processed as numeric with slider. Defaults to None.
        text_columns (List[str], optional): Columns that should be processed as text with pattern search. Defaults to None.
        excluded_filter_columns (List[str], optional): Columns to exclude from the filter options. Defaults to None.
        container (optional): Custom container to place the explorer in. Defaults to None.
        
    Returns:
        pd.DataFrame: The filtered dataframe
        
    Note:
        When specifying column types explicitly, the function will prioritize them in this order:
        1. Date columns (fecha_columns)
        2. Multiselect columns (multiselect_columns)
        3. Numeric columns (numeric_columns)
        4. Text columns (text_columns)
        
        For columns not specified in any of these lists, the function will infer the type based on the data.
    """
    # Import warnings to suppress specific pandas warnings related to datetime parsing
    import warnings
    
    if multiselect_columns is None:
        multiselect_columns = []
        
    if fecha_columns is None:
        fecha_columns = ['Fecha Factura', 'Fecha Recepción', 'Fecha Pagado', 'Fecha Autorización']  # Valores por defecto
        
    if excluded_filter_columns is None:
        excluded_filter_columns = []

    # Initialize session state for this explorer_id if not already present
    if explorer_id not in st.session_state:
        st.session_state[explorer_id] = {}
    
    # Helper to get value from session state or a default
    def get_session_state_value(key_suffix: str, default: Any) -> Any:
        return st.session_state[explorer_id].get(key_suffix, default)

    # Helper to set value in session state (used by on_change callbacks)
    def set_session_state_value(key_suffix: str, widget_key: str):
        st.session_state[explorer_id][key_suffix] = st.session_state[widget_key]

    df_filtered = df.copy()
        
    # Suppress the specific warning about datetime format inference
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message="Could not infer format")
        
        # Process datetime columns
        for col in df_filtered.columns:
            # Handle existing datetime columns - just remove timezone if present
            if is_datetime64_any_dtype(df_filtered[col]):
                try:
                    if df_filtered[col].dt.tz is not None:
                        df_filtered[col] = df_filtered[col].dt.tz_localize(None)
                except (AttributeError, TypeError):
                    pass
            
            # Only attempt conversion on object/string columns that might contain dates
            elif is_object_dtype(df_filtered[col]):
                # Skip conversion attempt for columns unlikely to be dates based on column name
                if not any(date_hint in col.lower() for date_hint in ['fecha', 'date', 'time']):
                    continue
                    
                # Try specific formats first for more consistent parsing
                date_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y']
                
                for date_format in date_formats:
                    try:
                        df_filtered[col] = pd.to_datetime(df_filtered[col], format=date_format, errors='coerce')
                        # If we successfully parsed at least 50% of non-null values, keep the conversion
                        non_null_before = df[col].notna().sum()
                        non_null_after = df_filtered[col].notna().sum()
                        if non_null_before > 0 and non_null_after >= 0.5 * non_null_before:
                            break
                        else:
                            # Revert conversion if too many NaN values were introduced
                            df_filtered[col] = df[col].copy()
                    except Exception:
                        continue

    # Use provided container or default to st
    ui = container if container is not None else st
    
    modification_container = ui.container()

    with modification_container:
        cols_to_filter_state_key = "_columns_to_filter_selection"
        widget_key_cols_filter = f"{explorer_id}_{cols_to_filter_state_key}_widget"
        
        # Ensure stored columns are valid for the current df
        stored_columns = get_session_state_value(cols_to_filter_state_key, [])
        valid_stored_columns = [col for col in stored_columns if col in df_filtered.columns]

        # Filtrar las opciones disponibles excluyendo las columnas especificadas
        available_filter_columns = [col for col in df_filtered.columns if col not in excluded_filter_columns]
        
        to_filter_columns = st.multiselect(
            "Filtrar tabla de datos por columnas:",
            options=available_filter_columns,
            default=[col for col in valid_stored_columns if col not in excluded_filter_columns],
            key=widget_key_cols_filter,
            placeholder="Selecciona columnas para filtrar",
            on_change=set_session_state_value, args=(cols_to_filter_state_key, widget_key_cols_filter)
        )
        # Persist initial or current state of the column selector immediately
        st.session_state[explorer_id][cols_to_filter_state_key] = to_filter_columns
        
        # Ordenar las columnas para procesar primero las fechas
        # Esto asegura que todas las columnas de fecha se procesen con prioridad
        sorted_columns = []
        date_columns = []
        other_columns = []
        
        for col in to_filter_columns:
            if col in fecha_columns:
                date_columns.append(col)
            else:
                other_columns.append(col)
                
        # Primero procesamos las fechas, luego el resto
        sorted_columns = date_columns + other_columns
        
        # Reemplazar to_filter_columns con la versión ordenada
        to_filter_columns = sorted_columns

        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            left.write("↳")

            # Define a unique key for session state for this specific filter, independent of widget key
            filter_state_key = f"_filter_value_{column}"
            # Define a unique key for the widget itself
            widget_key = f"{explorer_id}_{column}_widget"
            
            # Determinar el tipo de columna basado en las listas proporcionadas
            is_date_column = column in (fecha_columns or [])
            is_numeric_column = column in (numeric_columns or [])
            is_text_column = column in (text_columns or [])
            force_multiselect = column in (multiselect_columns or [])
            
            # Solo inferir tipo si no fue explicitamente definido
            if not any([is_date_column, is_numeric_column, is_text_column, force_multiselect]):
                is_low_cardinality = is_categorical_dtype(df_filtered[column]) or df_filtered[column].nunique() < 10
                # Inferir tipo basado en datos si no fue especificado
                if is_datetime64_any_dtype(df_filtered[column]):
                    is_date_column = True
                elif is_numeric_dtype(df_filtered[column]):
                    is_numeric_column = True
                elif is_string_dtype(df_filtered[column]) or is_object_dtype(df_filtered[column]):
                    is_text_column = True
            else:
                # Para multiselect necesitamos saber si tiene baja cardinalidad aunque su tipo sea explícito
                is_low_cardinality = is_categorical_dtype(df_filtered[column]) or df_filtered[column].nunique() < 10

            # Procesamiento basado en tipo de columna asignado explícitamente
            # Prioridad: fecha -> multiselect -> numérico -> texto
            if is_date_column:
                # Para todas las columnas de fecha, intentamos la conversión si no son ya datetime
                try:
                    # Primero, aseguremos que la columna sea de tipo string para la conversión
                    if not is_string_dtype(df_filtered[column]) and not is_datetime64_any_dtype(df_filtered[column]):
                        df_filtered[column] = df_filtered[column].astype(str)
                    
                    # Si ya es datetime, no necesitamos convertirlo
                    if not is_datetime64_any_dtype(df_filtered[column]):
                        with warnings.catch_warnings():
                            warnings.filterwarnings("ignore", category=UserWarning)
                            # Probar múltiples formatos explícitos antes de caer en inferencia automática
                            date_formats = ['%Y-%m-%d %H:%M:%S%z', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d', 
                                          '%d/%m/%Y %H:%M:%S', '%d/%m/%Y']
                            
                            converted = False
                            for fmt in date_formats:
                                try:
                                    df_filtered[column] = pd.to_datetime(df_filtered[column], format=fmt, errors='coerce')
                                    if not df_filtered[column].isna().all():  # Si al menos algunos valores se convirtieron
                                        converted = True
                                        break
                                except:
                                    continue
                            
                            # Si ningún formato específico funcionó, intentar inferencia automática
                            if not converted:
                                df_filtered[column] = pd.to_datetime(df_filtered[column], errors='coerce')
                    
                    col_series_datetime = df_filtered[column].dropna()
                    if col_series_datetime.empty:
                        right.warning(f"Columna '{column}' no contiene fechas válidas.")
                        continue
                        
                    # Asegurar que tenemos fechas válidas
                    min_date, max_date = col_series_datetime.min(), col_series_datetime.max()
                    
                    if pd.isna(min_date) or pd.isna(max_date) or min_date > max_date:
                        right.warning(f"Columna '{column}' tiene un rango de fechas inválido.")
                        continue
                    
                    default_dates = get_session_state_value(filter_state_key, (min_date, max_date))
                    # Ensure default_dates are Timestamp objects for comparison if they came from session state
                    try:
                        sd = pd.to_datetime(default_dates[0])
                        ed = pd.to_datetime(default_dates[1])
                    except: # If conversion fails, reset to min/max
                        sd, ed = min_date, max_date
    
                    clamped_default_dates = (max(min_date, sd), min(max_date, ed))
                    if clamped_default_dates[0] > clamped_default_dates[1]: clamped_default_dates = (min_date, max_date)
    
                    current_filter_values = right.date_input(
                        f"Valores para {column}", value=clamped_default_dates, format="DD/MM/YYYY",
                        min_value=min_date, max_value=max_date, key=widget_key,
                        on_change=set_session_state_value, args=(filter_state_key, widget_key)
                    )
                    st.session_state[explorer_id][filter_state_key] = current_filter_values # Persist
                    if len(current_filter_values) == 2:
                        start_date_ts, end_date_ts = pd.to_datetime(current_filter_values[0]), pd.to_datetime(current_filter_values[1])
                        end_date_inclusive = end_date_ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                        df_filtered = df_filtered.loc[df_filtered[column].between(start_date_ts, end_date_inclusive)]
                except Exception as e:
                    right.warning(f"Error al procesar la columna de fecha '{column}': {str(e)}")
                    
            elif force_multiselect or (is_low_cardinality and not (is_numeric_column or is_date_column)):
                unique_values = sorted(list(pd.Series(df_filtered[column].unique()).dropna()))
                default_selection = get_session_state_value(filter_state_key, [])
                valid_selection = [val for val in default_selection if val in unique_values]
                
                current_filter_values = right.multiselect(
                    f"Valores para {column}",
                    options=unique_values,
                    placeholder=f"Selecciona {column} para filtrar",
                    default=valid_selection,
                    key=widget_key,
                    on_change=set_session_state_value, args=(filter_state_key, widget_key)
                )
                st.session_state[explorer_id][filter_state_key] = current_filter_values # Persist current value
                if current_filter_values: # Apply filter if there's a selection
                    df_filtered = df_filtered[df_filtered[column].isin(current_filter_values)]

            elif is_numeric_column or is_numeric_dtype(df_filtered[column]):
                # Convertir a numérico si es posible
                try:
                    col_series_numeric = pd.to_numeric(df_filtered[column], errors='coerce').dropna()
                    if col_series_numeric.empty:
                        right.warning(f"Columna '{column}' no contiene valores numéricos válidos después de la conversión.")
                        continue
                    _min, _max = float(col_series_numeric.min()), float(col_series_numeric.max())
                    step = (_max - _min) / 100 if _max > _min and _max - _min > 0 else 1.0
                    if _min == _max: step = 0.1 # Avoid step being 0 if min=max
    
                    default_range = get_session_state_value(filter_state_key, (_min, _max))
                    clamped_default_range = (max(_min, default_range[0]), min(_max, default_range[1]))
                    if clamped_default_range[0] > clamped_default_range[1]: clamped_default_range = (_min, _max)
    
                    current_filter_values = right.slider(
                        f"Valores para {column}",
                        min_value=_min, max_value=_max, value=clamped_default_range, step=step,
                        key=widget_key,
                        on_change=set_session_state_value, args=(filter_state_key, widget_key)
                    )
                    st.session_state[explorer_id][filter_state_key] = current_filter_values # Persist
                    df_filtered = df_filtered[df_filtered[column].between(*current_filter_values)]
                except Exception as e:
                    right.warning(f"Error al procesar la columna numérica '{column}': {str(e)}")
                    continue
                
            elif is_text_column or is_string_dtype(df_filtered[column]) or is_object_dtype(df_filtered[column]):
                # Campo de texto para filtrar por contenido
                default_text = get_session_state_value(filter_state_key, "")
                current_filter_text = right.text_input(
                    f"Buscar patrón(es) en {column} (separar con '|' para múltiples patrones)",
                    value=default_text, key=widget_key,
                    on_change=set_session_state_value, args=(filter_state_key, widget_key)
                )
                st.session_state[explorer_id][filter_state_key] = current_filter_text # Persist
                if current_filter_text:
                    patterns = [p.strip() for p in current_filter_text.split('|') if p.strip()]
                    if patterns:
                        regex_pattern = '|'.join(patterns)
                        try:
                            df_filtered = df_filtered[df_filtered[column].astype(str).str.contains(regex_pattern, case=case, na=False, regex=True)]
                        except Exception as e:
                            right.warning(f"Error aplicando filtro regex en {column}: {e}")
            else:
                right.warning(f"Tipo de columna '{column}' no reconocido: {df_filtered[column].dtype}") 


    return df_filtered
