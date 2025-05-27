import streamlit as st
import pandas as pd
import os
from utils.authentication import Authentication
from utils.dataframe_utils import custom_dataframe_explorer
from utils.config import get_config
from utils.download_utils import GestorDescargas, preparar_ruta_destino, CombinadorPDF, sanitizar_nombre_archivo
import tempfile
import time

MAX_ROWS_TO_DISPLAY = 10000


# --- Verificar si los datos están completamente cargados ---
if not st.session_state.get("data_fully_loaded", False):
    st.warning("Los datos aún se están cargando. Por favor, espera en la página principal hasta que se complete la carga.")
    st.stop()

# Importar funciones centralizadas para acceso a datos
from pages.utils_3 import get_data_loader_instance, get_column_mapping

# Load custom CSS
def load_css():
    with open(os.path.join("assets", "styles.css")) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

try:
    load_css()
except Exception as e:
    st.warning(f"Unable to load custom CSS: {e}")

# Initialize authentication
authentication = Authentication()
# Main page content
st.title(":file_cabinet: Base de Datos Portal de Proveedores")
st.markdown("Impulsado por PalmaTerra 360 | Módulo Facturas :speech_balloon: :llama:", help="Explora y consulta la base de datos de la empresa")

# --- Carga de Datos Principal desde el cargador centralizado mejorado ---
try:
    # Usar el cargador centralizado mejorado para obtener los datos
    data_loader = get_data_loader_instance(load_data=False)
    
    # Obtener los DataFrames de las tablas necesarias
    # El nuevo data loader devuelve ambos dataframes que necesitamos
    data = data_loader.get_desglosado_dataframe()
    data_contabilidad = data_loader.get_contabilidad_dataframe()

    if data is not None:
        st.info(f"Shape of 'data' (desglosado): {data.shape}")
    else:
        st.info("'data' (desglosado) is None")
    if data_contabilidad is not None:
        st.info(f"Shape of 'data_contabilidad': {data_contabilidad.shape}")
    else:
        st.info("'data_contabilidad' is None")

    if data is None or data.empty:
        st.warning("No se pudieron obtener los datos. Por favor regresa a la página principal para completar la carga.")
        # Configurar valores por defecto para evitar errores posteriores
        data = pd.DataFrame()
        column_mapping = {}
    else:
        # Obtener el mapeo de columnas desde la configuración centralizada
        column_mapping = get_column_mapping()

except Exception as e:
    st.error(f"Ocurrió un error inesperado durante la obtención de datos: {str(e)}")
    # Fallback seguro
    data = pd.DataFrame()
    column_mapping = {}

# Contenido principal - Sección de Datos con pestañas
if not data.empty:
    # Usar los datos originales (sin filtros del sidebar) para el dataframe
    # Listas de columnas para formateo
    currency_columns = ["Subtotal", "Precio Unitario", "Total", "IVA 16%", "Descuento", "Retención IVA", "Retención ISR", "ISH", 'Venta Tasa 0%', 'Venta Tasa 16%']
    URL_columns = ["Factura", "Orden de Compra", "Remisión"] # Asumiendo que esta columna existe y no fue renombrada

    # Renombrar ANTES de pasar a dataframe_explorer
    display_data_renamed = data.copy()
    display_data_renamed.rename(columns=column_mapping, inplace=True, errors='ignore') # Ignorar si alguna columna del map no existe

    # Crear pestañas para visualización de datos
    tab1, tab2 = st.tabs(["Desglosado", "Concentrado"])
    
    # Pestaña 1 - Vista Desglosada (original)
    with tab1:
        # Usar custom_dataframe_explorer con los datos renombrados y configuración específica
        multiselect_filter_cols = ["Obra", "Subcategoría", "Categoría", "Proveedor", "Residente", "Tipo Gasto"] # Define columns for multiselect
        filtered_df = custom_dataframe_explorer(
            df=display_data_renamed, 
            explorer_id="desglosado_explorer", # Added explorer_id
            case=False, 
            multiselect_columns=multiselect_filter_cols
        )

        # ---- Crear diccionario de configuración de columnas ----
        column_config_dict = {}

        # Configuración para columnas de moneda
        for col in currency_columns:
            if col in filtered_df.columns: # Verificar que la columna exista en el DF filtrado/renombrado
                column_config_dict[col] = st.column_config.NumberColumn(
                    label=col, # Usar el nombre actual de la columna como etiqueta
                    format="dollar",
                    help=f"Valores monetarios en {col}"
                )

        # Configuración para columnas de URL
        for col in URL_columns:
             if col in filtered_df.columns: # Verificar que la columna exista
                 column_config_dict[col] = st.column_config.LinkColumn(
                     label=col, # Usar el nombre actual de la columna como etiqueta
                     display_text="Ver PDF", # Texto que se mostrará en el enlace
                     help=f"Enlace al documento PDF ({col})",
                     width="small"
                 )
        # -------------------------------------------------------

        # Mostrar el dataframe filtrado CON configuración de columnas
        st.dataframe(
            filtered_df.head(MAX_ROWS_TO_DISPLAY),
            use_container_width=True,
            column_config=column_config_dict, # Aplicar la configuración
            height=525  # Aumentar la altura para aprovechar el espacio
        )
        if len(filtered_df) > MAX_ROWS_TO_DISPLAY:
            st.caption(f"ℹ️ Mostrando las primeras {MAX_ROWS_TO_DISPLAY:,} filas de {len(filtered_df):,} registros totales después de aplicar filtros.")

        # Información sobre el número de filas mostradas con estilo mejorado
        st.info(f"📊 Mostrando {len(filtered_df):,} de {len(display_data_renamed):,} registros según los filtros aplicados")

    # Pestaña 2 - Vista Concentrada (agrupada por xml_uuid)
    with tab2:
        # Crear una copia de los datos originales para trabajar
        df_concentrado = data_contabilidad.copy()

        # Renombrar las columnas para la visualización
        df_concentrado_renamed = df_concentrado.copy()
        df_concentrado_renamed.rename(columns=column_mapping, inplace=True, errors='ignore')
            
        # Filtrar datos concentrados
        filtered_concentrado = custom_dataframe_explorer(
            df=df_concentrado_renamed,
            explorer_id="concentrado_explorer", # Added explorer_id
            case=False,
            multiselect_columns=multiselect_filter_cols
        )
            
        # Crear diccionario de configuración de columnas para la vista concentrada
        concentrado_config_dict = {}
            
        # Configuración para columnas de moneda
        for col in currency_columns:
            if col in filtered_concentrado.columns:
                    concentrado_config_dict[col] = st.column_config.NumberColumn(
                        label=col,
                        format="dollar",
                        help=f"Valores monetarios en {col} (agrupados)"
                    )
            
        # Configuración para columnas de URL
        for col in URL_columns:
            if col in filtered_concentrado.columns:
                concentrado_config_dict[col] = st.column_config.LinkColumn(
                    label=col,
                    display_text="Ver PDF",
                        help=f"Enlace al documento PDF ({col})",
                        width="small"
                    )

        # Inicializar variables en session state si no existen
        if 'saved_selections' not in st.session_state:
            st.session_state.saved_selections = pd.DataFrame()
            
        # Mostrar el dataframe concentrado con selección de filas habilitada
        selection_event = st.dataframe(
            filtered_concentrado.head(MAX_ROWS_TO_DISPLAY),
            use_container_width=True,
            column_config=concentrado_config_dict,
            height=525,
            # on_select="rerun", # Temporarily commented out for debugging WebSocketClosedError
            selection_mode="multi-row"
        )
        if len(filtered_concentrado) > MAX_ROWS_TO_DISPLAY:
            st.caption(f"ℹ️ Mostrando las primeras {MAX_ROWS_TO_DISPLAY:,} facturas de {len(filtered_concentrado):,} totales después de aplicar filtros.")
        
        st.info(f"📊 Mostrando {len(filtered_concentrado):,} de {len(df_concentrado_renamed):,} facturas")

        # Mover los botones de selección a la barra lateral
        # Sección de selección de filas en la barra lateral
        st.sidebar.subheader("Herramientas de selección")
        
        # Botones de selección en la misma fila
        col_save, col_clear = st.sidebar.columns(2)
        
        # Botón para guardar selección
        if col_save.button("💾 Guardar filas seleccionadas", key="save_selection"):
            if selection_event.selection and len(selection_event.selection.rows) > 0:
                # Obtener los índices de las filas seleccionadas
                selected_indices = selection_event.selection.rows
                # Filtrar el dataframe para obtener solo las filas seleccionadas
                selected_rows = filtered_concentrado.iloc[selected_indices]
                
                # Guardar las filas seleccionadas en session state
                # Si ya hay filas guardadas, concatenar con las nuevas
                if not st.session_state.saved_selections.empty:
                    # Concatenar sin duplicados (basado en índices o alguna columna única)
                    combined = pd.concat([st.session_state.saved_selections, selected_rows])
                    # Eliminar duplicados si existe alguna columna que sea identificador único
                    if 'xml_uuid' in combined.columns:
                        st.session_state.saved_selections = combined.drop_duplicates(subset=['xml_uuid'])
                    else:
                        st.session_state.saved_selections = combined.drop_duplicates()
                else:
                    st.session_state.saved_selections = selected_rows
                
                st.sidebar.success(f"✅ {len(selected_rows)} filas guardadas correctamente. Total: {len(st.session_state.saved_selections)} filas")
            else:
                st.sidebar.warning("⚠️ No hay filas seleccionadas para guardar")

        # Botón para limpiar selección
        if col_clear.button("🗑️ Limpiar filas seleccionadas", key="clear_selection"):
            st.session_state.saved_selections = pd.DataFrame()
            st.sidebar.success("✅ Tabla temporal limpiada correctamente")
            
        # Separador para filtros automáticos
        st.sidebar.markdown("---")
        st.sidebar.subheader("Filtros automáticos")
        
        # Botón para filtrar y guardar facturas con descuento > 0
        col1, col2 = st.sidebar.columns(2)
        
        if col1.button("🔍 Facturas con descuento", key="filter_descuento"):
            try:
                if 'Descuento' in filtered_concentrado.columns:
                    # Filtrar facturas con descuento > 0
                    df_con_descuento = filtered_concentrado[filtered_concentrado['Descuento'] > 0].copy()
                    
                    # Mostrar el número de facturas encontradas
                    num_facturas = len(df_con_descuento)
                    
                    if num_facturas > 0:
                        # Guardar en el dataframe temporal
                        if not st.session_state.saved_selections.empty:
                            # Concatenar con selecciones existentes
                            combined = pd.concat([st.session_state.saved_selections, df_con_descuento])
                            # Eliminar duplicados si existe una columna identificadora única
                            if 'xml_uuid' in combined.columns:
                                st.session_state.saved_selections = combined.drop_duplicates(subset=['xml_uuid'])
                            else:
                                st.session_state.saved_selections = combined.drop_duplicates()
                        else:
                            # Guardar directamente si no hay selecciones previas
                            st.session_state.saved_selections = df_con_descuento
                        
                        st.sidebar.success(f"✅ Se guardaron {num_facturas} facturas con descuento")
                    else:
                        st.sidebar.warning("No se encontraron facturas con descuento")
                else:
                    st.sidebar.error("La columna 'Descuento' no existe en el dataframe")
            except Exception as e:
                st.sidebar.error(f"Error al filtrar: {e}")
        
        # Botón para filtrar y guardar facturas con retenciones (ISH, Retención IVA, Retención ISR)
        if col2.button("🔍 Facturas con retenciones", key="filter_retenciones"):

            try:
                # Verificar que las columnas necesarias existen en el dataframe
                columnas_retenciones = ['ISH', 'Retención IVA', 'Retención ISR']
                columnas_existentes = [col for col in columnas_retenciones if col in filtered_concentrado.columns]
                
                if len(columnas_existentes) > 0:
                    # Crear máscara para cada columna existente
                    mask = pd.Series([False] * len(filtered_concentrado), index=filtered_concentrado.index)
                    
                    # Sumar las máscaras para cada columna
                    for col in columnas_existentes:
                        mask = mask | (filtered_concentrado[col] != 0)
                    
                    # Filtrar las facturas que cumplen con al menos una condición
                    df_con_retenciones = filtered_concentrado[mask].copy()
                    
                    # Mostrar el número de facturas encontradas
                    num_facturas = len(df_con_retenciones)
                    
                    if num_facturas > 0:
                        # Guardar en el dataframe temporal
                        if not st.session_state.saved_selections.empty:
                            # Concatenar con selecciones existentes
                            combined = pd.concat([st.session_state.saved_selections, df_con_retenciones])
                            # Eliminar duplicados si existe una columna identificadora única
                            if 'xml_uuid' in combined.columns:
                                st.session_state.saved_selections = combined.drop_duplicates(subset=['xml_uuid'])
                            else:
                                st.session_state.saved_selections = combined.drop_duplicates()
                        else:
                            # Guardar directamente si no hay selecciones previas
                            st.session_state.saved_selections = df_con_retenciones
                        
                        st.sidebar.success(f"✅ Se guardaron {num_facturas} facturas con retenciones")
                    else:
                        st.sidebar.warning("No se encontraron facturas con retenciones")
                else:
                    st.sidebar.error("No se encontraron las columnas de retenciones en el dataframe")
            except Exception as e:
                st.sidebar.error(f"Error al filtrar: {e}")
        
        # Botón para filtrar y guardar facturas con Tasa 0 y facturas en USD
        col3, col4 = st.sidebar.columns(2)
        if col3.button("🔍 Facturas con Impuesto Tasa 0", key="filter_tasa0"):

            try:
                columnas_tasas = ['Venta Tasa 0%', 'Venta Tasa 16%']
                columnas_existentes = [col for col in columnas_tasas if col in filtered_concentrado.columns]
                
                if len(columnas_existentes) > 0:
                    # Crear máscaras para las condiciones
                    mask_total = pd.Series([False] * len(filtered_concentrado), index=filtered_concentrado.index)
                    
                    # Condición 1: Venta Tasa 0% > 0
                    if 'Venta Tasa 0%' in filtered_concentrado.columns:
                        mask_tasa0_mayor_0 = filtered_concentrado['Venta Tasa 0%'] > 0
                        mask_total = mask_total | mask_tasa0_mayor_0
                    
                    # Condición 2: Ambas tasas son 0
                    if all(col in filtered_concentrado.columns for col in columnas_tasas):
                        mask_ambas_0 = (filtered_concentrado['Venta Tasa 0%'] == 0) & (filtered_concentrado['Venta Tasa 16%'] == 0)
                        mask_total = mask_total | mask_ambas_0
                    
                    # Filtrar las facturas que cumplen con alguna condición
                    df_tasa0 = filtered_concentrado[mask_total].copy()
                    
                    # Mostrar el número de facturas encontradas
                    num_facturas = len(df_tasa0)
                    
                    if num_facturas > 0:
                        # Guardar en el dataframe temporal
                        if not st.session_state.saved_selections.empty:
                            # Concatenar con selecciones existentes
                            combined = pd.concat([st.session_state.saved_selections, df_tasa0])
                            # Eliminar duplicados
                            if 'xml_uuid' in combined.columns:
                                st.session_state.saved_selections = combined.drop_duplicates(subset=['xml_uuid'])
                            else:
                                st.session_state.saved_selections = combined.drop_duplicates()
                        else:
                            # Guardar directamente si no hay selecciones previas
                            st.session_state.saved_selections = df_tasa0
                        
                        st.sidebar.success(f"✅ Se guardaron {num_facturas} facturas con Tasa 0")
                    else:
                        st.sidebar.warning("No se encontraron facturas que cumplan las condiciones de Impuesto Tasa 0")
                else:
                    st.sidebar.error("No se encontraron las columnas de tasas en el dataframe")
            except Exception as e:
                st.sidebar.error(f"Error al filtrar: {e}")
        
        # Botón para filtrar y guardar facturas en moneda USD
        if col4.button("💲 Facturas con moneda en USD", key="filter_usd"):
            try:
                if 'Moneda' in filtered_concentrado.columns:
                    # Filtrar facturas en USD
                    df_usd = filtered_concentrado[filtered_concentrado['Moneda'] == 'USD'].copy()
                    
                    # Mostrar el número de facturas encontradas
                    num_facturas = len(df_usd)
                    
                    if num_facturas > 0:
                        # Guardar en el dataframe temporal
                        if not st.session_state.saved_selections.empty:
                            # Concatenar con selecciones existentes
                            combined = pd.concat([st.session_state.saved_selections, df_usd])
                            # Eliminar duplicados
                            if 'xml_uuid' in combined.columns:
                                st.session_state.saved_selections = combined.drop_duplicates(subset=['xml_uuid'])
                            else:
                                st.session_state.saved_selections = combined.drop_duplicates()
                        else:
                            # Guardar directamente si no hay selecciones previas
                            st.session_state.saved_selections = df_usd
                        
                        st.sidebar.success(f"✅ Se guardaron {num_facturas} facturas en USD")
                    else:
                        st.sidebar.warning("No se encontraron facturas en USD")
                else:
                    st.sidebar.error("No se encontró la columna 'Moneda' en el dataframe")
            except Exception as e:
                st.sidebar.error(f"Error al filtrar: {e}")
        
        # Separador para sección de descargas
        st.sidebar.markdown("---")
        st.sidebar.subheader("Descargas de PDFs")
        
        # Inicializar variables en session_state para el dialogo de descargas
        if 'download_source' not in st.session_state:
            st.session_state.download_source = 'filtered_concentrado'
        if 'download_columns' not in st.session_state:
            st.session_state.download_columns = []
        if 'download_mode' not in st.session_state:
            st.session_state.download_mode = 'combined'
        if 'download_dir' not in st.session_state:
            # Definir un directorio de descarga predeterminado para evitar errores
            fecha_actual = time.strftime('%Y-%m-%d')
            # Verificar que base_downloads_dir exista en session_state
            if 'base_downloads_dir' not in st.session_state:
                home_dir = os.path.expanduser('~')
                st.session_state.base_downloads_dir = os.path.join(home_dir, 'Downloads')
            st.session_state.download_dir = os.path.join(st.session_state.base_downloads_dir, f'Facturas_StreamlPT_{fecha_actual}')  # Actualizado a 'combined' porque 'separate' ya no existe
        
        # Preparar ruta base de la carpeta de descargas (pero no crearla aún)
        if 'base_downloads_dir' not in st.session_state:
            home_dir = os.path.expanduser('~')
            st.session_state.base_downloads_dir = os.path.join(home_dir, 'Downloads')
            
        if 'download_progress' not in st.session_state:
            st.session_state.download_progress = 0
        if 'download_results' not in st.session_state:
            st.session_state.download_results = None
        
        # Definir la función de diálogo para descargas
        @st.dialog("Configuración de descarga", width="large")
        def configurar_descarga():
            st.subheader("Configuración de descarga de PDFs")
            
            # Selección de fuente de datos
            st.session_state.download_source = st.radio(
                "Seleccione la fuente de datos:",
                options=[
                    'filtered_concentrado', 
                    'filtered_df', 
                    'saved_selections'
                ],
                format_func=lambda x: {
                    'filtered_concentrado': "Facturas (Concentrado)", 
                    'filtered_df': "Facturas (Desglosado)", 
                    'saved_selections': "Selección temporal"
                }[x],
                index=0
            )
            
            # Mapeo de opciones a dataframes
            data_source_map = {
                'filtered_concentrado': filtered_concentrado,
                'filtered_df': filtered_df,
                'saved_selections': st.session_state.saved_selections
            }
            
            selected_df = data_source_map[st.session_state.download_source]
            rows_count = len(selected_df) if not selected_df.empty else 0
            
            st.info(f"📊 Fuente seleccionada: {rows_count:,} registros disponibles")
            
            # No mostrar opciones si el dataframe está vacío
            if not selected_df.empty:
                # Selección de columnas para descargar
                available_url_columns = [col for col in URL_columns if col in selected_df.columns]
                
                if available_url_columns:
                    st.session_state.download_columns = st.multiselect(
                        "Seleccione las columnas para descargar:",
                        options=available_url_columns,
                        default=available_url_columns[:1] if available_url_columns else []
                    )
                    
                    if st.session_state.download_columns:
                        # Opciones de descarga de PDFs
                        st.session_state.download_mode = st.radio(
                            "Modo de descarga:",
                            options=['combined', 'joined'],
                            format_func=lambda x: {
                                'combined': "PDF por fila", 
                                'joined': "Un solo archivo"
                            }[x],
                            index=0,
                            help="'PDF por fila' combina los documentos de cada fila en un solo PDF. 'Un solo archivo' une todos los PDFs en un único archivo."
                        )
                        
                        # Mostrar información breve y concisa sobre las opciones (actualizado)
                        st.caption(f"📁 Destino: {st.session_state.download_dir}")
                        
                        # Una sola línea informativa según el modo seleccionado
                        if st.session_state.download_mode == 'combined':
                            st.info("🗞️ Se creará un PDF por cada fila")
                        elif st.session_state.download_mode == 'joined':
                            st.info("🗞️ Se creará un único archivo PDF con todos los documentos")
                        
                        # Botón para iniciar la descarga
                        if st.button("Iniciar descarga", key="start_download", type="primary"):
                            # Actualizar la carpeta de destino para las descargas al iniciar la descarga
                            fecha_actual = time.strftime('%Y-%m-%d')
                            facturas_dir = os.path.join(st.session_state.base_downloads_dir, f'Facturas_StreamlPT_{fecha_actual}')
                            st.session_state.download_dir = facturas_dir
                            
                            # Obtener los links del dataframe seleccionado, con tratamiento especial para filtered_df
                            if st.session_state.download_source == 'filtered_df':
                                # Para el dataframe desglosado, agrupar por Factura para evitar links duplicados
                                df_unique = selected_df.drop_duplicates(subset=['Factura']).copy()
                                df_to_process = df_unique
                            else:
                                # Para otros dataframes, procesar todos los registros directamente
                                df_to_process = selected_df.copy()
                            
                            # Crear gestor de descargas
                            gestor = GestorDescargas(max_workers=10)
                            
                            # Añadir descargas a la cola
                            file_count = 0
                            
                            # Asegurar que la carpeta de destino exista y sea accesible
                            try:
                                os.makedirs(st.session_state.download_dir, exist_ok=True)
                            except Exception as e:
                                # En caso de error, intentar con el directorio de descargas
                                home_dir = os.path.expanduser('~')
                                st.session_state.download_dir = os.path.join(home_dir, 'Downloads', 'Facturas_StreamlPT')
                                os.makedirs(st.session_state.download_dir, exist_ok=True)
                            
                            # Procesar según modo seleccionado - solo 'combined' y 'joined'
                            # Código común para ambos modos
                            # Crear barra de progreso para mostrar el avance del proceso
                            progress_bar = st.progress(0)
                            progress_text = st.empty()
                            
                            if st.session_state.download_mode == 'combined':
                                progress_text.write("Procesando documentos para combinarlos por fila...")
                            else:  # modo 'joined'
                                progress_text.write("Procesando documentos para combinarlos en un solo archivo...")
                                
                            # Verificar disponibilidad de columnas 'Obra' y 'Proveedor' para nombrado
                            nombre_cols_disponibles = all(col in df_to_process.columns for col in ['Obra', 'Proveedor'])
                            if not nombre_cols_disponibles:
                                st.warning("⚠️ No se encontraron las columnas 'Obra' y 'Proveedor'. Se usarán nombres genéricos.")

                            # Crear gestor para combinar PDFs
                            combinador = CombinadorPDF()
                            if not combinador.can_combine:
                                st.error("PyPDF2 no está instalado. Por favor, ejecuta 'pip install PyPDF2' y reinicia la aplicación.")
                                return
                                    
                            # Contador para filas procesadas y nombre de archivos
                            counter = 0
                            total_docs = 0
                            combined_results = []
                            all_combined_pdfs = []  # Para el modo 'joined'
                            
                            # Obtener el total de filas para calcular el progreso
                            total_rows = len(df_to_process)
                            if total_rows == 0:
                                progress_bar.progress(100)  # No hay filas, progreso completo
                                progress_text.write("No hay documentos para procesar")
                            else:
                                progress_text.write(f"Procesando {total_rows} filas de documentos...")
                                
                            # Procesar cada fila y combinar sus documentos en un solo PDF
                            for idx, row in df_to_process.iterrows():
                                counter += 1
                                
                                # Actualizar la barra de progreso - fase 1 (50% del proceso total)
                                # La primera mitad del progreso es para el procesamiento de filas
                                progress_percent = min(counter / total_rows * 50, 50)
                                progress_bar.progress(int(progress_percent))
                                progress_text.write(f"Procesando fila {counter} de {total_rows}...")
                                
                                # Recopilar URLs a combinar para esta fila
                                urls = []
                                for col in st.session_state.download_columns:
                                    if pd.notna(row.get(col)) and row.get(col) and isinstance(row.get(col), str):
                                        urls.append(row.get(col))
                                    
                                if not urls:
                                    continue  # No hay URLs en esta fila, continuar con la siguiente
                                
                                # Generar nombre de archivo basado en Obra y Proveedor (si están disponibles)
                                if nombre_cols_disponibles:
                                    obra = sanitizar_nombre_archivo(str(row.get('Obra', '')))
                                    proveedor = sanitizar_nombre_archivo(str(row.get('Proveedor', '')))
                                    filename = f"{counter}_{obra}_{proveedor}.pdf"
                                else:
                                    # Usar UUID o fecha + contador como identificador único
                                    if 'xml_uuid' in row:
                                        filename = f"{counter}_{sanitizar_nombre_archivo(str(row.get('xml_uuid')))}.pdf"
                                    else:
                                        filename = f"{counter}_combinado_{time.strftime('%H%M%S')}_{idx}.pdf"
                                
                                # Ruta destino para el PDF combinado
                                ruta_combinada = os.path.join(st.session_state.download_dir, filename)
                                
                                # Combinar los PDFs
                                exito, mensaje = combinador.combinar_pdfs(urls, ruta_combinada)
                                combined_results.append({
                                    'exito': exito,
                                    'mensaje': mensaje,
                                    'ruta': ruta_combinada,
                                    'urls': urls
                                })
                                
                                if exito:
                                    total_docs += 1
                                    # Guardar la ruta para el modo 'joined'
                                    if st.session_state.download_mode == 'joined' and os.path.exists(ruta_combinada):
                                        all_combined_pdfs.append(ruta_combinada)
                            
                            # Actualizar progreso al 50% después de procesar todas las filas
                            progress_bar.progress(50)
                            progress_text.write("Fase 1 completada: Procesamiento de documentos")
                            
                            # Mostrar resultados en formato de tabla
                            if combined_results:
                                # Si estamos en modo 'joined', combinar todos los PDFs en uno solo
                                if st.session_state.download_mode == 'joined' and all_combined_pdfs:
                                    progress_text.write("Uniendo todos los PDFs en un solo archivo...")
                                    # Nombre para el archivo final
                                    timestamp = time.strftime('%Y%m%d_%H%M%S')
                                    ruta_final = os.path.join(st.session_state.download_dir, f"concentrado_{timestamp}.pdf")
                                    
                                    # Combinar todos los PDFs generados en uno solo
                                    from PyPDF2 import PdfMerger  # Importar aquí para asegurar que esté disponible
                                    merger = PdfMerger()
                                    archivos_unidos = 0
                                    total_archivos = len(all_combined_pdfs)
                                    
                                    try:
                                        for i, pdf_path in enumerate(all_combined_pdfs):
                                            try:
                                                merger.append(pdf_path)
                                                archivos_unidos += 1
                                                
                                                # Actualizar progreso - fase 2 (del 50% al 90%)
                                                # La segunda fase representa la unión de PDFs
                                                progress_percent = 50 + min((i + 1) / total_archivos * 40, 40)
                                                progress_bar.progress(int(progress_percent))
                                                progress_text.write(f"Uniendo PDF {i+1} de {total_archivos}...")
                                            except Exception:
                                                pass  # Ignoramos errores silenciosamente
                                        
                                        # Guardar el archivo final
                                        progress_bar.progress(90)
                                        progress_text.write("Finalizando: guardando archivo PDF unido...")
                                        merger.write(ruta_final)
                                        merger.close()
                                        
                                        # Eliminar automáticamente los archivos individuales
                                        for pdf_path in all_combined_pdfs:
                                            try:
                                                os.remove(pdf_path)
                                            except Exception:
                                                pass
                                        
                                        # Completar la barra de progreso
                                        progress_bar.progress(100)
                                        progress_text.write(f"Completado: {archivos_unidos} PDFs unidos")
                                        
                                        # Un solo mensaje de éxito
                                        st.success(f"✅ Se han unido {archivos_unidos} PDFs en un solo archivo: {os.path.basename(ruta_final)}")
                                    
                                    except Exception as e:
                                        st.error(f"Error al combinar todos los PDFs: {e}")
                                            
                                elif not st.session_state.download_mode == 'joined':
                                    # Completar la barra de progreso para modo 'combined'
                                    progress_bar.progress(100)
                                    progress_text.write(f"Completado: {total_docs} PDFs generados")
                                    
                                    # Mensaje de éxito para modo 'combined' (PDF por fila)
                                    st.success(f"✅ Proceso finalizado: {total_docs} PDFs generados de {counter} filas procesadas")
                                
                                # Mostrar errores solo si existen
                                errores = [res for res in combined_results if not res['exito']]
                                if errores:
                                    with st.expander(f"Ver {len(errores)} errores"):
                                        for i, res in enumerate(errores):
                                            st.error(f"{res['mensaje']}")
                            else:
                                st.info("No se encontraron documentos para procesar")
                            
                            # Ejecutar descargas de manera más simple
                            with st.spinner("Procesando documentos..."):
                                resultados = gestor.ejecutar_descargas()
                                st.session_state.download_results = resultados
                                
                            # Mostrar errores solo si existen
                            errores = [r for r in resultados if not r.get('exito', False)]
                            if errores:
                                with st.expander(f"Ver {len(errores)} errores"):
                                    for e in errores:
                                        st.error(f"Error en {e.get('url', 'descarga')}: {e.get('mensaje', 'Error desconocido')}")
                            
                            # Botón para cerrar el diálogo después de completar la descarga
                            if st.button("Cerrar", key="close_dialog"):
                                st.rerun()
                    else:
                        st.warning("Debe seleccionar al menos una columna para descargar")
                else:
                    st.error("No se encontraron columnas con URLs en este dataframe")
            else:
                st.error("El dataframe seleccionado está vacío. No hay datos para descargar.")
                if st.button("Cerrar", key="close_dialog_empty"):
                    st.rerun()
        
        # Botón para abrir el diálogo de descargas
        if st.sidebar.button("📥 Descargar PDFs", key="open_download_dialog"):
            configurar_descarga()



        # # Mostrar información sobre las filas actualmente seleccionadas
        # if selection_event.selection and len(selection_event.selection.rows) > 0:
        #     st.info(f"🔍 {len(selection_event.selection.rows)} filas seleccionadas actualmente")

        # Mostrar las filas guardadas en la selección temporal si existen
        if not st.session_state.saved_selections.empty:
            st.subheader("Filas guardadas en selección temporal")
            # Agregar mensaje con el número total de filas guardadas
            num_filas = len(st.session_state.saved_selections)
            
            st.dataframe(
                st.session_state.saved_selections,
                use_container_width=True,
                column_config=concentrado_config_dict,
                height=300
            )
            st.info(f"📊 Mostrando {num_filas:,} facturas seleccionadas")


###
#Aqui empezó el código nuevo
####

else:
    # Si no hay datos o hubo un error, mostrar mensaje
    st.warning("No hay datos disponibles para mostrar. Comprueba la conexión y configuración de Supabase.")

# Simplificar CSS para evitar conflictos
st.markdown("""
<style>
.simple-header {
    margin-bottom: 1rem;
}
</style>
""", unsafe_allow_html=True)

# # Add help information
# st.markdown("---")
# with st.expander(":information_source: Ayuda para la Base de Datos"):
#     st.markdown("""
#     ### Consejos para usar esta página:
    
#     - **Filtros de datos**: Utiliza los controles de filtrado para buscar información específica.
#     - **Búsqueda**: Usa el campo de búsqueda para encontrar registros por texto.
#     - **Ordenar**: Haz clic en los encabezados de columna para ordenar los datos.
#     - **Exportar**: Puedes seleccionar datos y copiarlos para usarlos en otras aplicaciones.
    
#     Para obtener más ayuda, contacta al administrador del sistema.
#     """)
