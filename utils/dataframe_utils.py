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

def custom_dataframe_explorer(df: pd.DataFrame, explorer_id: str, case: bool = True, multiselect_columns: Optional[List[str]] = None, container=None) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns, with customized
    filtering options for specific text columns. Uses st.session_state to persist filters.

    Args:
        df (pd.DataFrame): Original dataframe.
        explorer_id (str): A unique identifier for this dataframe explorer instance to
                           isolate its filters in st.session_state.
        case (bool, optional): If True, text inputs (regex) will be case sensitive. Defaults to True.
        multiselect_columns (Optional[List[str]], optional): List of column names that should 
            use st.multiselect for filtering, even if they are text type with high cardinality.
            Defaults to None.
        container (optional): Container where to place the filter controls. For example, st.sidebar.
                            If None, uses st (main container). Defaults to None.

    Returns:
        pd.DataFrame: Filtered dataframe.
    """
    if multiselect_columns is None:
        multiselect_columns = []

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

    # Try to convert potential datetimes into standard format (do this once on the copy)
    for col in df_filtered.columns:
        if is_object_dtype(df_filtered[col]):
            try:
                df_filtered[col] = pd.to_datetime(df_filtered[col])
            except Exception:
                pass # Ignore columns that cannot be converted
        if is_datetime64_any_dtype(df_filtered[col]):
            try:
                if df_filtered[col].dt.tz is not None:
                    df_filtered[col] = df_filtered[col].dt.tz_localize(None) # Ensure timezone-naive
            except AttributeError: # Handle cases where conversion might fail or Series is empty
                pass

    # Use provided container or default to st
    ui = container if container is not None else st
    
    modification_container = ui.container()

    with modification_container:
        cols_to_filter_state_key = "_columns_to_filter_selection"
        widget_key_cols_filter = f"{explorer_id}_{cols_to_filter_state_key}_widget"
        
        # Ensure stored columns are valid for the current df
        stored_columns = get_session_state_value(cols_to_filter_state_key, [])
        valid_stored_columns = [col for col in stored_columns if col in df_filtered.columns]

        to_filter_columns = st.multiselect(
            "Filtrar dataframe en columnas:",
            options=list(df_filtered.columns),
            default=valid_stored_columns,
            key=widget_key_cols_filter,
            on_change=set_session_state_value, args=(cols_to_filter_state_key, widget_key_cols_filter)
        )
        # Persist initial or current state of the column selector immediately
        st.session_state[explorer_id][cols_to_filter_state_key] = to_filter_columns

        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            left.write("↳")

            # Define a unique key for session state for this specific filter, independent of widget key
            filter_state_key = f"_filter_value_{column}"
            # Define a unique key for the widget itself
            widget_key = f"{explorer_id}_{column}_widget"

            is_low_cardinality = is_categorical_dtype(df_filtered[column]) or df_filtered[column].nunique() < 10
            force_multiselect = column in multiselect_columns and (is_string_dtype(df_filtered[column]) or is_object_dtype(df_filtered[column]))

            if is_low_cardinality or force_multiselect:
                unique_values = sorted(list(pd.Series(df_filtered[column].unique()).dropna()))
                default_selection = get_session_state_value(filter_state_key, [])
                valid_selection = [val for val in default_selection if val in unique_values]
                
                current_filter_values = right.multiselect(
                    f"Valores para {column}",
                    options=unique_values,
                    default=valid_selection,
                    key=widget_key,
                    on_change=set_session_state_value, args=(filter_state_key, widget_key)
                )
                st.session_state[explorer_id][filter_state_key] = current_filter_values # Persist current value
                if current_filter_values: # Apply filter if there's a selection
                    df_filtered = df_filtered[df_filtered[column].isin(current_filter_values)]

            elif is_numeric_dtype(df_filtered[column]):
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

            elif is_datetime64_any_dtype(df_filtered[column]):
                col_series_datetime = df_filtered[column].dropna()
                if col_series_datetime.empty:
                    right.warning(f"Columna '{column}' no contiene fechas válidas.")
                    continue
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

                try:
                    current_filter_values = right.date_input(
                        f"Valores para {column}", value=clamped_default_dates, 
                        min_value=min_date, max_value=max_date, key=widget_key,
                        on_change=set_session_state_value, args=(filter_state_key, widget_key)
                    )
                    st.session_state[explorer_id][filter_state_key] = current_filter_values # Persist
                    if len(current_filter_values) == 2:
                        start_date_ts, end_date_ts = pd.to_datetime(current_filter_values[0]), pd.to_datetime(current_filter_values[1])
                        end_date_inclusive = end_date_ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                        df_filtered = df_filtered.loc[df_filtered[column].between(start_date_ts, end_date_inclusive)]
                except Exception as e:
                    right.warning(f"Error al crear filtro de fecha para {column}: {e}")

            elif is_string_dtype(df_filtered[column]) or is_object_dtype(df_filtered[column]):
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
                right.write(f"Filtrado no implementado para el tipo: {df_filtered[column].dtype}")

    return df_filtered
