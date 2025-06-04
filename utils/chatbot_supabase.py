import streamlit as st
import pandas as pd
from supabase import create_client, Client
import json

# --- Funciones de caché global para Supabase Chatbot ---

@st.cache_resource
def init_chatbot_supabase_client():
    """Inicializa y cachea el cliente Supabase para el chatbot.
    
    Esta función está cacheada con @st.cache_resource para que solo se inicialice
    una vez y se comparta entre todas las páginas de la aplicación.
    """
    try:
        url = st.secrets["supabase"]["url"]
        key = st.secrets["supabase"]["key"]
        return create_client(url, key)
    except Exception as e:
        st.error(f"Error crítico al inicializar Supabase Client para Chatbot: {e}")
        return None


@st.cache_data(ttl=3600)  # Cache for 1 hour
def map_obras_to_cuenta_gasto(_client: Client):
    """
    Mapea cada obra con su cuenta_gasto correspondiente utilizando la tabla vista_cuentas_unicas_filtradas.
    
    Args:
        _client: Cliente Supabase inicializado
        
    Returns:
        Un diccionario donde la clave es el nombre de la obra y el valor es su cuenta_gasto correspondiente.
    """
    obra_to_cuenta = {}
    
    if not _client:
        st.warning("Cliente Supabase no inicializado, no se puede crear el mapeo obra-cuenta_gasto.")
        return obra_to_cuenta
    
    try:
        # Obtener los datos de la tabla vista_cuentas_unicas_filtradas con obra y cuenta_gasto
        mapping_data = _client.table("vista_cuentas_unicas_filtradas").select("obra,cuenta_gasto").execute().data
        
        # Crear el diccionario de mapeo
        for item in mapping_data:
            if item.get("obra") is not None and item.get("cuenta_gasto") is not None:
                obra_to_cuenta[item["obra"]] = item["cuenta_gasto"]
    
    except Exception as e:
        st.warning(f"Error al crear mapeo obra-cuenta_gasto desde Supabase: {e}")
    
    return obra_to_cuenta


@st.cache_data(ttl=3600, show_spinner=False)
def get_chatbot_filter_options(_client: Client):
    """Obtiene y cachea las opciones de filtro para el chatbot.
    
    Esta función está cacheada con @st.cache_data para reducir la carga en la base de datos.
    Los datos se actualizan cada hora (ttl=3600).
    
    Args:
        _client: Cliente Supabase inicializado
        
    Returns:
        Un diccionario con las opciones de filtro (obras, proveedores, subcategorias, categorias)
        y el mapeo entre obras y cuenta_gasto.
    """
    options = {
        'obras': [],
        'proveedores': [],
        'subcategorias': [],
        'categorias': [],
        'obra_to_cuenta_gasto': {}  # Nueva clave para el mapeo
    }
    
    if not _client:
        st.warning("Cliente Supabase no inicializado, no se pueden cargar filtros.")
        return options
        
    try:
        # Using .execute().data and then processing for uniqueness and sorting
        obras_data = _client.table("vista_cuentas_unicas_filtradas").select("obra").execute().data
        options['obras'] = sorted(list(set(item["obra"] for item in obras_data if item["obra"] is not None)))

        proveedores_data = _client.table("portal_desglosado").select("proveedor").execute().data
        options['proveedores'] = sorted(list(set(item["proveedor"] for item in proveedores_data if item["proveedor"] is not None)))

        subcategorias_data = _client.table("categorias_subcategorias").select("subcategoria").execute().data
        options['subcategorias'] = sorted(list(set(item["subcategoria"] for item in subcategorias_data if item["subcategoria"] is not None)))

        categorias_data = _client.table("categorias_subcategorias").select("categoria_id").execute().data
        options['categorias'] = sorted(list(set(str(item["categoria_id"]) for item in categorias_data if item["categoria_id"] is not None)))  # Ensure string conversion
        
        # Obtener el mapeo de obra a cuenta_gasto
        obra_to_cuenta = map_obras_to_cuenta_gasto(_client)
        options['obra_to_cuenta_gasto'] = obra_to_cuenta
        
        # Crear el mapeo inverso de cuenta_gasto a obra
        cuenta_to_obra = {}
        for obra, cuenta_gasto in obra_to_cuenta.items():
            # Convertir cuenta_gasto a string para usar como clave
            cuenta_gasto_str = str(cuenta_gasto)
            cuenta_to_obra[cuenta_gasto_str] = obra
        
        options['cuenta_to_obra'] = cuenta_to_obra
        
    except Exception as e:
        st.warning(f"Error al cargar opciones de filtro para Chatbot desde Supabase: {e}")
    
    return options




@st.cache_data(ttl=600, show_spinner=False)  # Cache for 10 minutes
def get_filtered_data_multiselect(_client: Client, table_name: str, select_columns: str = "*", obras_seleccionadas=None, proveedores_seleccionados=None, fecha_inicio=None, fecha_fin=None, fecha_rango=None, estatus_seleccionados=None, fecha_seleccionada=None):
    """Obtiene datos filtrados del portal desglosado basado en selecciones del usuario.
    
    Esta función permite obtener datos específicos basados en filtros aplicados por el usuario,
    evitando cargar todos los datos en memoria cuando solo se necesita un subconjunto.
    
    Args:
        _client: Cliente Supabase inicializado
        table_name: Nombre de la tabla a consultar en Supabase.
        select_columns: String con las columnas a seleccionar, separadas por comas (ej: "col1,col2,col3"). Por defecto es "*".
        obras_seleccionadas: Lista de obras seleccionadas (opcional)
        proveedores_seleccionados: Lista de proveedores seleccionados (opcional)
        fecha_inicio: Fecha de inicio para filtrar (opcional)
        fecha_fin: Fecha de fin para filtrar (opcional)
        fecha_rango: Tupla con fecha de inicio y fin para filtrar (opcional, mantenido por compatibilidad)
        estatus_seleccionados: Lista de estatus seleccionados (opcional, puede ser 'Pagada', 'Proceso de Pago', 'RevisaRes')
        fecha_seleccionada: Tipo de fecha seleccionada (opcional, puede ser 'Fecha Factura', 'Fecha Recepción', 'Fecha Pagado', 'Fecha Autorización')
        
    Returns:
        DataFrame de pandas con los datos filtrados
    """
    if not _client:
        st.error("Cliente Supabase no inicializado")
        return pd.DataFrame()
        
    try:
        print (
        "obras_seleccionadas: \n",obras_seleccionadas,
        "proveedores_seleccionados",proveedores_seleccionados,
        "fecha_rango",fecha_rango,
        "estatus_seleccionados",estatus_seleccionados,
        "fecha_seleccionada",fecha_seleccionada)

        # Mapear obras a cuentas_gasto si es necesario
        cuentas_gasto = []
        if obras_seleccionadas:
            # Obtener el mapeo de obra a cuenta_gasto
            filtros = get_chatbot_filter_options(_client)
            obra_to_cuenta = filtros.get('obra_to_cuenta_gasto', {})
            
            # Convertir obras seleccionadas a cuentas_gasto
            for obra in obras_seleccionadas:
                cuenta_gasto = obra_to_cuenta.get(obra)
                if cuenta_gasto is not None:
                    cuentas_gasto.append(str(cuenta_gasto))
        print ("cuentas_gasto",cuentas_gasto)
        
        # Iniciar la consulta básica con las columnas especificadas
        print(f"DEBUG - Consultando tabla: {table_name} con columnas: {select_columns}")
        query = _client.table(table_name).select(select_columns)
        
        # Aplicar filtros solo si hay selecciones (no vacías)
        # Filtro para cuenta_gasto
        if cuentas_gasto:
            query = query.in_("cuenta_gasto", cuentas_gasto)
        
        # Filtro para proveedor
        if proveedores_seleccionados:
            query = query.in_("proveedor", proveedores_seleccionados)
            
        # Filtro para estatus
        if estatus_seleccionados:
            query = query.in_("estatus", estatus_seleccionados)
                # Filtro para el rango de fechas
        # Apply date range filter across all date columns
        # Primero verificamos si se pasaron fechas individuales
        if fecha_inicio is not None and fecha_fin is not None:
            print(f"DEBUG - Fecha rango seleccionado: {fecha_inicio} a {fecha_fin}")
        # Si no, verificamos si existe la tupla de fecha_rango por compatibilidad
        elif fecha_rango and isinstance(fecha_rango, tuple) and len(fecha_rango) == 2:
            fecha_inicio, fecha_fin = fecha_rango
            print(f"DEBUG - Fecha rango seleccionado (desde tupla): {fecha_inicio} a {fecha_fin}")
        
        # Solo procesamos si ambas fechas están presentes
        if fecha_inicio is not None and fecha_fin is not None:
            fecha_inicio_str = f"{fecha_inicio.isoformat()}T00:00:00+00:00"
            fecha_fin_str = f"{fecha_fin.isoformat()}T23:59:59+00:00"
            print(f"DEBUG - Fecha inicio formateada: {fecha_inicio_str}")
            print(f"DEBUG - Fecha fin formateada: {fecha_fin_str}")
            
            print("DEBUG - Creating multiple date filters for OR condition")
            
            # Aplicar filtro OR para cubrir todos los campos de fecha
            # Crear filtros para cada campo de fecha
            fecha_factura_filter = f"and(fecha_factura.gte.{fecha_inicio_str},fecha_factura.lte.{fecha_fin_str})"
            fecha_recepcion_filter = f"and(fecha_recepcion.gte.{fecha_inicio_str},fecha_recepcion.lte.{fecha_fin_str})"
            fecha_pagada_filter = f"and(fecha_pagada.gte.{fecha_inicio_str},fecha_pagada.lte.{fecha_fin_str})"
            fecha_autorizacion_filter = f"and(fecha_autorizacion.gte.{fecha_inicio_str},fecha_autorizacion.lte.{fecha_fin_str})"
            
            # Mapa para mapear la selección de fecha a la columna correspondiente
            fecha_columna_map = {
                'Fecha Factura': 'fecha_factura',
                'Fecha Recepción': 'fecha_recepcion',
                'Fecha Pagado': 'fecha_pagada',
                'Fecha Autorización': 'fecha_autorizacion'
            }
            
            # Construct a list of individual AND conditions
            date_conditions = []
            
            # Si se seleccionó un tipo específico de fecha, solo filtrar por esa columna
            if fecha_seleccionada and fecha_seleccionada in fecha_columna_map:
                columna = fecha_columna_map[fecha_seleccionada]
                date_conditions.append(f"and({columna}.gte.{fecha_inicio_str},{columna}.lte.{fecha_fin_str})")
                print(f"DEBUG - Filtrando por la columna de fecha: {columna}")
            else:
                # Si no se seleccionó ningún tipo específico, filtrar por todas las columnas de fecha (comportamiento actual)
                date_conditions.append(f"and(fecha_factura.gte.{fecha_inicio_str},fecha_factura.lte.{fecha_fin_str})")
                date_conditions.append(f"and(fecha_recepcion.gte.{fecha_inicio_str},fecha_recepcion.lte.{fecha_fin_str})")
                date_conditions.append(f"and(fecha_pagada.gte.{fecha_inicio_str},fecha_pagada.lte.{fecha_fin_str})")
                date_conditions.append(f"and(fecha_autorizacion.gte.{fecha_inicio_str},fecha_autorizacion.lte.{fecha_fin_str})")
                print("DEBUG - Filtrando por todas las columnas de fecha")
            
            # Join these conditions with a comma for the OR filter
            final_date_filter_string = ",".join(date_conditions)
            print(f"DEBUG - Final date filter string for OR: {final_date_filter_string}")
            
            # Try printing the query URL before executing the or_ operation
            print(f"DEBUG - Query before date filter: {query}")
            
            # Apply the single OR condition
            query = query.or_(final_date_filter_string)
        else:
            print("DEBUG - No se aplicó filtro de fecha porque una o ambas fechas son None")
        
        # Print query after date filter to see what changed
        print(f"DEBUG - Query after date filter: {query}")
            
        print("DEBUG - Ejecutando query final...")
        response = query.execute()
        
        print(f"DEBUG - Cantidad de registros encontrados: {len(response.data) if response.data else 0}")
        if response.data:
            # Convertir a DataFrame
            df = pd.DataFrame(response.data)
            
            # Asegurar que las columnas de fecha sean de tipo datetime
            for col in ['fecha_factura', 'fecha_recepcion', 'fecha_pagada', 'fecha_autorizacion']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Asegurar que las columnas numéricas sean de tipo numérico
            for col in ['cantidad', 'precio_unitario', 'subtotal', 'descuento', 'venta_tasa_0', 
                        'venta_tasa_16', 'total_iva', 'total_ish', 'retencion_iva', 'retencion_isr', 
                        'total']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al obtener datos filtrados: {e}")
        return pd.DataFrame()
