import streamlit as st
import pandas as pd
import numpy as np
import os
import plotly.express as px
import matplotlib.pyplot as plt
from utils.authentication import Authentication
from utils.config import get_config
from pages.utils_3 import get_data_loader_instance
from supabase import create_client, Client

# --- Verificar si los datos están completamente cargados ---
if not st.session_state.get("data_fully_loaded", False):
    st.warning("Los datos aún se están cargando. Por favor, espera en la página principal hasta que se complete la carga.")
    st.stop()

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
st.title(":chart_with_upwards_trend: Visualización de Datos")
st.markdown("Impulsado por PalmaTerra 360 | Módulo Facturas :speech_balloon: :llama:", help="Explora y visualiza datos de la empresa")


# Importar las funciones centralizadas desde el módulo de utilidades
from utils.chatbot_supabase import init_chatbot_supabase_client, get_chatbot_filter_options

# Usar las funciones centralizadas con caché
supabase_client_chatbot = init_chatbot_supabase_client()


if supabase_client_chatbot:
    chatbot_filter_opts = get_chatbot_filter_options(supabase_client_chatbot)
else:
    # Fallback to empty options if Supabase client failed
    chatbot_filter_opts = {key: [] for key in ['obras', 'proveedores', 'subcategorias', 'categorias']}
    st.error("No se pudo conectar a Supabase. Los filtros no estarán disponibles.")

# Contenido principal

# Inicializar variables para los dataframes
data = pd.DataFrame()
filtered_data = pd.DataFrame()

# Función para obtener datos filtrados de Supabase
def get_filtered_data(client, categorias, subcategorias, cuentas_gasto):
    try:
        # Iniciar la consulta básica
        query = client.table("portal_desglosado").select(
            "obra, categoria_id, subcategoria, fecha_factura, total, proveedor, cuenta_gasto"
        )
        
        # Aplicar filtros solo si hay selecciones (no vacías)
        # Si la lista está vacía, no aplicamos el filtro para esa categoría
        if categorias:
            query = query.in_("categoria_id", categorias)
            
        if subcategorias:
            query = query.in_("subcategoria", subcategorias)
        
        if cuentas_gasto:
            query = query.in_("cuenta_gasto", cuentas_gasto)
        
        # Ejecutar la consulta
        response = query.execute()
        
        if response.data:
            # Convertir a DataFrame
            df = pd.DataFrame(response.data)
            
            # Asegurar que la columna fecha_factura sea de tipo datetime
            if 'fecha_factura' in df.columns:
                df['fecha_factura'] = pd.to_datetime(df['fecha_factura'], errors='coerce')
                
            # Asegurar que total sea numérico
            if 'total' in df.columns:
                df['total'] = pd.to_numeric(df['total'], errors='coerce')
                
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al obtener datos filtrados: {e}")
        return pd.DataFrame()

# Inicializar variables de estado si no existen
if 'viz_selected_categories' not in st.session_state:
    st.session_state.viz_selected_categories = []
if 'viz_selected_subcategorias' not in st.session_state:
    st.session_state.viz_selected_subcategorias = []
if 'viz_selected_obras' not in st.session_state:
    st.session_state.viz_selected_obras = []
if 'viz_data' not in st.session_state:
    st.session_state.viz_data = pd.DataFrame()
if 'viz_filtered_data' not in st.session_state:
    st.session_state.viz_filtered_data = pd.DataFrame()
if 'viz_submitted' not in st.session_state:
    st.session_state.viz_submitted = False
    
# Inicializar variables locales para evitar errores de referencia
data = st.session_state.viz_data
filtered_data = st.session_state.viz_filtered_data

# Sidebar con filtros globales y configuración
with st.sidebar:    
    # Crear un expander para los filtros
    with st.expander("Filtros de Visualización", expanded=False):
        # Usar un contenedor en lugar de un form con borde visible
        # Crear un formulario para que solo se procesen los inputs cuando se hace clic en el botón
        with st.form(key="filtros_form", border=False):
            # 1. Filtros para categorías
            st.subheader(":label: Categorías")
            
            all_categories = chatbot_filter_opts['categorias']
            
            selected_categories = st.multiselect(
                
                "Selección de Categorías",
                
                options=all_categories,
                
                default=st.session_state.viz_selected_categories,  # Usar valores guardados
                
                key="global_categorias_multiselect"
            
            )
            
            
            
            # 2. Filtro de Subcategorías
            
            st.subheader(":bookmark_tabs: Subcategorías")
            
            all_subcategorias = chatbot_filter_opts['subcategorias']
            
            selected_subcategorias = st.multiselect(
                
                "Selección de Subcategorías",
                
                options=all_subcategorias,
                
                default=st.session_state.viz_selected_subcategorias,  # Usar valores guardados
                
                key="global_subcats_multiselect"
            
            )
            
            
            
            # 3. Filtro de Obras
            
            st.subheader(":building_construction: Obras")
            
            all_obras = chatbot_filter_opts['obras']
            
            selected_obras = st.multiselect(
                
                "Selección de Obras",
                
                options=all_obras,
                
                default=st.session_state.viz_selected_obras,  # Usar valores guardados
                
                key="global_obras_multiselect"
            
            )
            
            
            
            # Botón para limpiar filtros
            col1, col2 = st.columns(2)
            with col1:
                clear_button = st.form_submit_button(":broom: Limpiar", use_container_width=True)
            
            # Botón para enviar el formulario
            with col2:
                submitted = st.form_submit_button(":bar_chart: Graficar", use_container_width=True, type="primary")
    

# Ya no necesitamos manejar valores por defecto de esta manera
# porque estamos utilizando session_state y respetamos los valores vacíos
# Esto evita que se seleccionen todas las opciones cuando el usuario no selecciona ninguna

# Procesamiento del botón Limpiar
if 'clear_button' in locals() and clear_button:
    # Limpiar los filtros y el estado
    st.session_state.viz_selected_categories = []
    st.session_state.viz_selected_subcategorias = []
    st.session_state.viz_selected_obras = []
    st.session_state.viz_data = pd.DataFrame()
    st.session_state.viz_filtered_data = pd.DataFrame()
    st.session_state.viz_submitted = False
    
    # Limpiar las variables locales también
    data = pd.DataFrame()
    filtered_data = pd.DataFrame()
    st.rerun()
    
# Cuando se hace clic en el botón de Graficar dentro del formulario
if ('submitted' in locals() and submitted) or st.session_state.viz_submitted:
    with st.spinner("Obteniendo datos filtrados..."):
        if supabase_client_chatbot:
            # Usar los valores de filtro correctos dependiendo del contexto
            if 'submitted' in locals() and submitted:
                # Si se presionó el botón de graficar, usar los valores seleccionados actualmente
                filter_categories = selected_categories
                filter_subcategorias = selected_subcategorias
                filter_obras = selected_obras
            else:
                # Si se está restaurando el estado, usar los valores guardados en session_state
                filter_categories = st.session_state.viz_selected_categories
                filter_subcategorias = st.session_state.viz_selected_subcategorias
                filter_obras = st.session_state.viz_selected_obras
                
            # Convertir las obras seleccionadas a sus cuentas_gasto correspondientes
            selected_cuentas_gasto = []
            for obra in filter_obras:
                if obra in chatbot_filter_opts['obra_to_cuenta_gasto']:
                    cuenta_gasto = chatbot_filter_opts['obra_to_cuenta_gasto'][obra]
                    selected_cuentas_gasto.append(cuenta_gasto)
            
            # Si se presionó el botón, actualizar el estado con nuevos valores
            if 'submitted' in locals() and submitted:
                # Actualizar el estado en session_state
                st.session_state.viz_selected_categories = selected_categories
                st.session_state.viz_selected_subcategorias = selected_subcategorias
                st.session_state.viz_selected_obras = selected_obras
                st.session_state.viz_submitted = True
                
                # Obtener los datos filtrados
                data = get_filtered_data(
                    supabase_client_chatbot, 
                    filter_categories, 
                    filter_subcategorias, 
                    selected_cuentas_gasto  # Pasar las cuentas_gasto en lugar de las obras
                )
                
                # Guardar los datos en session_state
                st.session_state.viz_data = data
                st.session_state.viz_filtered_data = data.copy()
            else:
                # Verificamos si hay datos guardados en session_state
                if not st.session_state.viz_data.empty:
                    # Obtener los datos guardados en session_state
                    data = get_filtered_data(
                        supabase_client_chatbot, 
                        filter_categories, 
                        filter_subcategorias, 
                        selected_cuentas_gasto  # Pasar las cuentas_gasto en lugar de las obras
                    )
                    
                    # Actualizar los datos en session_state
                    st.session_state.viz_data = data
                    st.session_state.viz_filtered_data = data.copy()
                else:
                    # No hay datos, usar DataFrames vacíos
                    data = pd.DataFrame()
                    st.session_state.viz_data = data
                    st.session_state.viz_filtered_data = data.copy()
                    
            # Asegurarse de que filtered_data siempre esté definida
            filtered_data = st.session_state.viz_filtered_data
            
            # Mostrar resumen de datos obtenidos
            if not data.empty:
                st.success(f"Datos obtenidos: {len(data)} registros")
                
                # Mostrar resumen de filtros aplicados
                filter_summary = []
                if st.session_state.viz_selected_categories:
                    filter_summary.append(f"Categorías: {len(st.session_state.viz_selected_categories)}")
                if st.session_state.viz_selected_subcategorias:
                    filter_summary.append(f"Subcategorías: {len(st.session_state.viz_selected_subcategorias)}")
                if st.session_state.viz_selected_obras:
                    filter_summary.append(f"Obras: {len(st.session_state.viz_selected_obras)}")
                    
                if filter_summary:
                    st.info(f"Filtros aplicados: {', '.join(filter_summary)}")
                else:
                    st.info("No se aplicaron filtros específicos. Se muestran todos los datos.")
            else:
                st.warning("No se encontraron datos con los filtros seleccionados.")
        else:
            st.error("No se pudo conectar a Supabase. Verifique la conexión.")

# Asegurarse de que filtered_data esté definida incluso fuera del flujo principal
if 'filtered_data' not in locals() or filtered_data is None:
    filtered_data = st.session_state.viz_filtered_data

# Mostrar información sobre los filtros aplicados
if not data.empty:
    
    # --- Sección de Gráficas por Categoría y Tendencia ---
    if not filtered_data.empty:
        # Create two columns for side-by-side charts
        bar_col, line_col = st.columns(2)
            
        # Bar Chart in first column - agrupado por OBRA
        with bar_col:
            # Verificar que existan las columnas necesarias para el gráfico de barras
            has_category = 'categoria_id' in filtered_data.columns
            has_total = 'total' in filtered_data.columns
            has_obra = 'obra' in filtered_data.columns
            
            if has_category and has_total:
                # Verificar si existe la columna cuenta_gasto
                has_cuenta_gasto = 'cuenta_gasto' in filtered_data.columns
                
                # Título según si tenemos datos de obra y cuenta_gasto
                title = "Total por Categoría y Obra" if has_obra else "Total por Categoría"
                st.subheader(title)
                
                # Preparar datos para el gráfico
                if has_obra and has_category and has_cuenta_gasto:
                    # Copiar los datos filtrados para no modificar el original
                    temp_data = filtered_data.copy()
                    
                    # Extraer la obra base de cada nombre de obra (quitar '/Servicios', '/Garantías', etc.)
                    temp_data['obra_base'] = temp_data['obra'].str.split('/').str[0].str.strip()
                    
                    # Agrupar por categoría y cuenta_gasto (para barras separadas)
                    # y luego por obra (para apilar variantes dentro de cada cuenta_gasto)
                    bar_chart_data = temp_data.groupby(['categoria_id', 'obra_base', 'obra'])['total'].sum().reset_index()
                    
                    # Usar barmode='group' para tener barras separadas por obra_base
                    # Las variantes se apilarán dentro de cada obra_base
                    barmode = 'group'
                elif has_obra and has_category:
                    # Agrupar por categoría y obra cuando no hay cuenta_gasto
                    bar_chart_data = filtered_data.groupby(['categoria_id', 'obra'])['total'].sum().reset_index()
                    barmode = 'group'  # Barras agrupadas (no apiladas)
                else:
                    # Solo agrupar por categoría
                    bar_chart_data = filtered_data.groupby('categoria_id')['total'].sum().reset_index()
                    barmode = 'relative'
                
                # Crear gráfico si hay datos
                if not bar_chart_data.empty:
                    # Crear el gráfico dependiendo del caso
                    if has_cuenta_gasto and 'cuenta_gasto' in bar_chart_data.columns:
                        # Crear gráfico con barras agrupadas por obra_base
                        fig_bar = px.bar(
                            bar_chart_data,
                            x='categoria_id',  # Categoría en el eje X
                            y='total',
                            color='obra',  # Colorear por obra completa (para distinguir variantes)
                            barmode=barmode,  # 'group' para barras agrupadas por obra_base
                            facet_row=None,  # No usar facetas
                            title=title,
                            labels={'categoria_id': 'Categoría', 'total': 'Total', 'obra': 'Obra', 'obra_base': 'Obra Base'},
                            # Agrupar por obra_base para tener barras independientes
                            # pero preservar la relación visual entre variantes de la misma obra
                            custom_data=['obra_base']
                        )
                    else:
                        # Gráfico estándar sin cuenta_gasto
                        fig_bar = px.bar(
                            bar_chart_data,
                            x='categoria_id',  # Usar categoría como eje X
                            y='total',
                            color='obra',  # Colorear por obra
                            barmode=barmode,
                            title=title,
                            labels={'categoria_id': 'Categoría', 'total': 'Total', 'obra': 'Obra'},
                        )
                    # Ajustar el diseño para mejorar la visualización
                    fig_bar.update_layout(
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        margin=dict(t=100),  # Mayor margen superior para la leyenda
                        xaxis_title="Categoría"  # Cambiar el título del eje X a 'Obra'
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)
                else:
                    st.info("No hay datos para el gráfico de barras con los filtros seleccionados.")
            else:
                missing_cols = []
                if not has_category: missing_cols.append("'categoria_id'")
                if not has_total: missing_cols.append("'total'")
                st.warning(f"Columnas necesarias ({', '.join(missing_cols)}) no encontradas para el gráfico de barras.")

        # Line Chart in second column
        with line_col:
            # Verificar que existan las columnas necesarias para el gráfico de líneas
            has_date = 'fecha_factura' in filtered_data.columns and not filtered_data['fecha_factura'].isna().all()
            has_total = 'total' in filtered_data.columns
            has_obra = 'obra' in filtered_data.columns
            
            if has_date and has_total:
                # Título según si tenemos datos de obra o no
                title = "Tendencia Temporal por Obra" if has_obra else "Tendencia Temporal"
                st.subheader(title)
                
                # Preparar datos para el gráfico
                temp_data = filtered_data.copy()
                
                # Asegurar que la fecha sea datetime y normalizar zona horaria
                if not pd.api.types.is_datetime64_any_dtype(temp_data['fecha_factura']):
                    temp_data['fecha_factura'] = pd.to_datetime(temp_data['fecha_factura'], errors='coerce')
                # Eliminar filas con fechas inválidas
                temp_data = temp_data[~temp_data['fecha_factura'].isna()]
                # Normalizar zona horaria (quitar zona horaria si existe)
                if hasattr(temp_data['fecha_factura'].dtype, 'tz'):
                    temp_data['fecha_factura'] = temp_data['fecha_factura'].dt.tz_localize(None)
                
                # Agrupar datos según si tenemos obra o no
                if has_obra:
                    # Agrupar por fecha y obra
                    line_data = temp_data.sort_values('fecha_factura').groupby(['fecha_factura', 'obra'])['total'].mean().reset_index()
                    color_by = 'obra'
                else:
                    # Solo agrupar por fecha
                    line_data = temp_data.sort_values('fecha_factura').groupby('fecha_factura')['total'].mean().reset_index()
                    color_by = None
                
                # Crear gráfico si hay datos
                if not line_data.empty:
                    # Configuración básica del gráfico
                    line_args = {
                        'x': 'fecha_factura',
                        'y': 'total',
                        'title': title,
                        'labels': {'fecha_factura': 'Fecha Factura', 'total': 'Total Promedio'}
                    }
                    
                    # Añadir color por obra si está disponible
                    if color_by:
                        line_args['color'] = color_by
                        line_args['labels']['obra'] = 'Obra'
                    
                    # Crear gráfico de líneas
                    fig_line = px.line(line_data, **line_args)
                    
                    # Ajustar diseño si hay múltiples líneas (por obra)
                    if color_by:
                        fig_line.update_layout(
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                            margin=dict(t=100)  # Mayor margen superior para la leyenda
                        )
                    
                    st.plotly_chart(fig_line, use_container_width=True)
                else:
                    st.info("No hay datos para el gráfico de líneas con los filtros seleccionados.")
            elif not has_date and 'fecha_factura' in data.columns:
                # La columna fecha existe en los datos originales pero no hay datos válidos después del filtrado
                st.info("No hay datos de fecha válidos con los filtros seleccionados para generar el gráfico de tendencia temporal.")
            elif not has_date:
                # La columna fecha no existe en los datos originales
                st.info("No hay datos de fecha válidos en la fuente original para generar el gráfico de tendencia temporal.")
            else:
                st.warning("Columna 'total' no encontrada para el gráfico de líneas.")

    else: # If filtered_data is empty after applying filters
        st.warning("No hay datos que coincidan con los filtros aplicados.")

    # --- Sección de Análisis por Subcategoría y Obra ---
    st.divider()
    st.subheader("📊 Análisis por Subcategoría y Obra")

    if not filtered_data.empty and 'subcategoria' in filtered_data.columns and 'total' in filtered_data.columns:
        # Ensure TOTAL is numeric before aggregation
        filtered_data_numeric = filtered_data.copy()
        filtered_data_numeric['total'] = pd.to_numeric(filtered_data_numeric['total'], errors='coerce')
        filtered_data_agg = filtered_data_numeric.dropna(subset=['total', 'subcategoria'])  # Drop rows where essential columns are NaN

        # Two columns layout for provider charts
        bar_h_col, heatmap_col = st.columns(2)

        with bar_h_col:
            # Top 15 subcategorias horizontal bar chart
            st.subheader("Top 15 Subcategorías por Total")

            # Aggregate data by subcategoria
            subcat_data = filtered_data_agg.groupby('subcategoria')['total'].sum().reset_index()
            subcat_data = subcat_data[subcat_data['total'] > 0]  # Consider only positive totals

            if not subcat_data.empty:
                # Sort by TOTAL descending and get top 15
                top_subcats_chart = subcat_data.sort_values('total', ascending=False).head(15)

                fig_bar_h = px.bar(
                    top_subcats_chart,
                    y='subcategoria',
                    x='total',
                    orientation='h',
                    title='Top 15 Subcategorías por Total',
                    labels={'subcategoria': 'Subcategoría', 'total': 'Total Acumulado'},
                    height=500,
                    color='total',
                    color_continuous_scale=px.colors.sequential.Blues,
                    text='total'  # Display total value on bars
                )
                fig_bar_h.update_traces(texttemplate='%{text:,.2f}', textposition='outside')
                fig_bar_h.update_layout(
                    yaxis={'categoryorder': 'total ascending'},
                    uniformtext_minsize=8, uniformtext_mode='hide'
                )
                st.plotly_chart(fig_bar_h, use_container_width=True)
            else:
                st.info("No hay datos de subcategorías con total positivo para mostrar.")

        with heatmap_col:
            # Heatmap of Subcategorias vs Obras
            st.subheader("Matriz de Subcategorías por Obra")

            if not filtered_data_agg.empty and 'obra' in filtered_data_agg.columns:
                # Create a cross-tabulation of Subcategorias vs Obras with sums of TOTAL
                try:
                    # Group by subcategoria and obra, summing the totals
                    heatmap_data = filtered_data_agg.pivot_table(
                        index='subcategoria',
                        columns='obra',
                        values='total',
                        aggfunc='sum',
                        fill_value=0
                    )

                    # Filter to include only top subcategorias (for readability)
                    top_subcats = subcat_data.sort_values('total', ascending=False).head(10)['subcategoria'].tolist()
                    heatmap_data = heatmap_data.loc[heatmap_data.index.isin(top_subcats)]

                    # Create heatmap with Plotly
                    fig_heatmap = px.imshow(
                        heatmap_data.values,
                        labels=dict(x="Obra", y="Subcategoría", color="Total"),
                        x=heatmap_data.columns.tolist(),
                        y=heatmap_data.index.tolist(),
                        color_continuous_scale="Blues",
                        title="Relación Subcategoría-Obra (Montos)"
                    )
                    
                    # Add text annotations with the values
                    fig_heatmap.update_traces(text=heatmap_data.values, texttemplate="%{z:,.0f}")
                    
                    # Adjust layout for better visualization
                    fig_heatmap.update_layout(
                        height=500,
                        margin=dict(l=50, r=50, t=80, b=50)
                    )
                    
                    st.plotly_chart(fig_heatmap, use_container_width=True)
                except Exception as e:
                    st.info(f"No se pudo generar la matriz de calor: {e}\nIntenta seleccionar menos filtros o más datos.")
            else:
                st.info("Datos insuficientes para generar la matriz de calor Subcategoría-Obra.")

    # Handle missing data or columns
    elif filtered_data.empty:
        st.warning("No hay datos que coincidan con los filtros aplicados.")
    elif not ('subcategoria' in filtered_data.columns and 'total' in filtered_data.columns):
        missing_cols = []
        if 'subcategoria' not in filtered_data.columns: missing_cols.append("'subcategoria'")
        if 'total' not in filtered_data.columns: missing_cols.append("'total'")
        st.warning(f"Columnas necesarias ({', '.join(missing_cols)}) no encontradas para generar gráficos de subcategoría.")

    # Mensaje si no hay datos originales
    if data.empty:
        st.warning("No hay datos disponibles para cargar.")


# La sección del mapa (tab3) ha sido eliminada según los requerimientos

# Simplificar CSS para evitar conflictos
st.markdown("""
<style>
.simple-header {
    margin-bottom: 1rem;
}
</style>
""", unsafe_allow_html=True)