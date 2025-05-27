import streamlit as st
import pandas as pd
import numpy as np
import os
import plotly.express as px
import matplotlib.pyplot as plt
from utils.authentication import Authentication
from utils.config import get_config
from pages.utils_3 import get_data_loader_instance

# --- Verificar si los datos están completamente cargados ---
if not st.session_state.get("data_fully_loaded", False):
    st.warning("Los datos aún se están cargando. Por favor, espera en la página principal hasta que se complete la carga.")
    st.stop()

# Importar funciones centralizadas para acceso a datos
from pages.utils_3 import get_column_mapping

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

# --- Carga de Datos Principal desde el cargador centralizado mejorado ---
try:
    # Usar el cargador centralizado mejorado para obtener los datos
    data_loader = get_data_loader_instance(load_data=False)
    
    # Obtener el DataFrame de la vista de desglosado para visualización
    data = data_loader.get_kiosko_dataframe()
    
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

# Contenido principal

# Sidebar con filtros globales y configuración
with st.sidebar:
    st.info(":gear: Filtros para Gráficos")
    
    # Solo mostrar filtros si hay datos disponibles
    if not data.empty:
        # 1. Filtros para categorías - mostrar TODAS las categorías disponibles
        if 'categoria_id' in data.columns:
            st.subheader(":label: Categorías")
            all_categories = sorted(data['categoria_id'].astype(str).unique())
            selected_categories = st.multiselect(
                "Selección de Categorías",
                options=all_categories,
                default=[],  # Comenzar vacío según preferencia del usuario
                key="global_categorias_multiselect"
            )
            # Si está vacío, considerar todas las categorías como seleccionadas
            if not selected_categories:
                selected_categories = all_categories
        else:
            selected_categories = []
        
        # 2. Filtro de Subcategorías - mostrar TODAS por defecto
        if 'subcategoria' in data.columns:
            st.subheader(":bookmark_tabs: Subcategorías")
            all_subcategorias = sorted(data['subcategoria'].astype(str).unique())
            
            # Mostrar todas las subcategorías disponibles siempre
            selected_subcategorias = st.multiselect(
                "Selección de Subcategorías",
                options=all_subcategorias,
                default=[],
                key="global_subcats_multiselect"
            )
            # Si no hay selección, usar todas las subcategorías
            if not selected_subcategorias:
                selected_subcategorias = all_subcategorias
        else:
            selected_subcategorias = []
            
        # 3. Filtro de rango de fechas (su visualización se muestra después de Obras)
        # Inicializar variables para fechas
        start_date_ts, end_date_ts = None, None
        min_date_obj, max_date_obj = None, None
        reset_dates = False
        
        # Obtener valores de fechas min/max (para inicialización y reseteo)
        if 'fecha_factura' in data.columns and not data['fecha_factura'].isna().all():
            min_date = data['fecha_factura'].min()
            max_date = data['fecha_factura'].max()
            if pd.notna(min_date) and pd.notna(max_date):
                min_date_obj = min_date.date() if isinstance(min_date, pd.Timestamp) else min_date
                max_date_obj = max_date.date() if isinstance(max_date, pd.Timestamp) else max_date
                
                # Inicializar fechas globales
                if 'reset_date_filter' in st.session_state and st.session_state.reset_date_filter:
                    # Si se pidió resetear, usar fechas completas
                    start_date_ts = pd.Timestamp(min_date_obj)
                    end_date_ts = pd.Timestamp(max_date_obj)
                    # Limpiar flag de reseteo
                    st.session_state.reset_date_filter = False
                    reset_dates = True
                else:
                    # Mostrar el widget de selección de fechas (aunque aún no se muestra visualmente)
                    date_key = "global_date_input"
                    current_dates = st.session_state.get(date_key, (min_date_obj, max_date_obj))
                    # Obtener fechas actuales del selector
                    if len(current_dates) == 2:
                        start_date_ts = pd.Timestamp(current_dates[0])
                        end_date_ts = pd.Timestamp(current_dates[1])
                    else:
                        start_date_ts = pd.Timestamp(min_date_obj)
                        end_date_ts = pd.Timestamp(max_date_obj)
        
        # Filtrar datos por rango de fechas para calcular top obras
        date_filtered_data = data.copy()
        if start_date_ts is not None and end_date_ts is not None:
            # Asegurar que la columna 'FECHA FACTURA' sea datetime para comparaciones
            if 'fecha_factura' in date_filtered_data.columns:
                # Convertir a datetime si no lo es
                if not pd.api.types.is_datetime64_any_dtype(date_filtered_data['fecha_factura']):
                    date_filtered_data['fecha_factura'] = pd.to_datetime(date_filtered_data['fecha_factura'], errors='coerce')
                # Filtrar solo filas con fechas válidas
                date_filtered_data = date_filtered_data[~date_filtered_data['fecha_factura'].isna()]
                
                # Normalizar zonas horarias
                # Convertir start_date_ts y end_date_ts a la zona horaria de los datos
                # Si tiene zona horaria, convertir a naive (sin zona horaria)
                if hasattr(date_filtered_data['fecha_factura'].dtype, 'tz'):
                    date_filtered_data['fecha_factura'] = date_filtered_data['fecha_factura'].dt.tz_localize(None)
                    # También asegurar que start_date_ts y end_date_ts no tengan zona horaria
                    if hasattr(start_date_ts, 'tz_localize'):
                        start_date_ts = start_date_ts.tz_localize(None)
                    if hasattr(end_date_ts, 'tz_localize'):
                        end_date_ts = end_date_ts.tz_localize(None)
                # O alternativamente, si los timestamps no tienen zona horaria pero los datos sí
                elif start_date_ts is not None and end_date_ts is not None:
                    # Asegurar que los timestamps para comparación sean naive
                    if hasattr(start_date_ts, 'tz') and start_date_ts.tz is not None:
                        start_date_ts = start_date_ts.tz_localize(None)
                    if hasattr(end_date_ts, 'tz') and end_date_ts.tz is not None:
                        end_date_ts = end_date_ts.tz_localize(None)
                
                # Aplicar filtro de fechas
                date_filtered_data = date_filtered_data[(date_filtered_data['fecha_factura'] >= start_date_ts) & 
                                                    (date_filtered_data['fecha_factura'] <= end_date_ts)]
                
        # 4. Filtro de Obras - mostrar todas pero usar top 10 por defecto según rango de fechas
        if 'obra' in data.columns:
            st.subheader(":building_construction: Obras")
            all_obras = sorted(data['obra'].astype(str).unique())
            
            # Calcular las top 10 obras por monto total BASADO EN EL RANGO DE FECHAS SELECCIONADO
            top_obras_df = date_filtered_data.copy()  # Usar datos filtrados por fecha
            top_obras_df['total'] = pd.to_numeric(top_obras_df['total'], errors='coerce')
            top_obras = top_obras_df.groupby('obra')['total'].sum().nlargest(10).index.tolist()  # 10 obras
            
            # Mostrar todas las obras disponibles en el selector
            selected_obras = st.multiselect(
                "Selección de Obras",
                options=all_obras,
                default=[],
                key="global_obras_multiselect"
            )
            
            # Si no hay selección, usar las top 10 obras del periodo seleccionado internamente
            if not selected_obras:
                # Obtener fechas actuales para el mensaje - esto asegura que las fechas mostradas
                # coincidan con las fechas realmente utilizadas para filtrar
                fecha_inicio = start_date_ts.strftime('%d/%m/%Y') if start_date_ts else 'inicio'
                fecha_fin = end_date_ts.strftime('%d/%m/%Y') if end_date_ts else 'fin'
                st.caption(f"Sin selección: mostrando las 10 obras principales por monto total del periodo {fecha_inicio} al {fecha_fin}")
                selected_obras = top_obras
        else:
            selected_obras = []
            
        # 5. Widget visual de Rango de Fechas (aunque la lógica se procesa antes)
        if 'fecha_factura' in data.columns and not data['fecha_factura'].isna().all() and min_date_obj is not None and max_date_obj is not None:
            st.subheader(":calendar: Rango de Fechas")
            
            # Inicializar la bandera de reseteo si no existe
            if 'reset_date_filter' not in st.session_state:
                st.session_state.reset_date_filter = False
            
            # Determinar el valor inicial a mostrar
            initial_dates = (min_date_obj, max_date_obj) if reset_dates else (start_date_ts.date(), end_date_ts.date())
            
            # Widget de selección de fechas
            date_range = st.date_input(
                "Periodo",
                value=initial_dates,
                min_value=min_date_obj,
                max_value=max_date_obj,
                key="global_date_input"
            )
            
            # Botón para resetear el filtro de fechas
            if st.button("🔄 Resetear rango de fechas", help="Volver al rango completo de fechas"):
                # En lugar de modificar el widget directamente, establecemos una bandera
                st.session_state.reset_date_filter = True
                st.rerun()
                
            # Actualizamos start_date_ts y end_date_ts con los valores del widget
            # (estos valores ya fueron usados antes para el cálculo de las obras principales)
            if len(date_range) == 2:
                # No necesitamos actualizar nada aquí, ya que estos valores
                # ya fueron procesados en la parte superior de la lógica
                pass
    
    # Separador para la configuración del mapa
    st.divider()
    
    # Eliminada la configuración específica del mapa ya que se ha quitado esa funcionalidad
    
    # authentication.logout()


# Preparar datos filtrados para los gráficos (solo se usarán en la pestaña de gráficos)
if not data.empty:
    filtered_data = data.copy()
    
    # Aplicar filtro de categorías
    if 'categoria_id' in filtered_data.columns and selected_categories:
        filtered_data = filtered_data[filtered_data['categoria_id'].astype(str).isin(map(str, selected_categories))]
    
    # Aplicar filtro de fechas
    if start_date_ts and end_date_ts and 'fecha_factura' in filtered_data.columns:
        # Asegurar que la columna fecha_factura sea datetime para comparaciones
        if not pd.api.types.is_datetime64_any_dtype(filtered_data['fecha_factura']):
            filtered_data['fecha_factura'] = pd.to_datetime(filtered_data['fecha_factura'], errors='coerce')
        # Eliminar filas con fechas inválidas
        filtered_data = filtered_data[~filtered_data['fecha_factura'].isna()]
        
        # Normalizar zonas horarias
        # Si tiene zona horaria, convertir a naive (sin zona horaria)
        if hasattr(filtered_data['fecha_factura'].dtype, 'tz'):
            filtered_data['fecha_factura'] = filtered_data['fecha_factura'].dt.tz_localize(None)
            # También asegurar que start_date_ts y end_date_ts no tengan zona horaria
            if hasattr(start_date_ts, 'tz_localize'):
                start_date_ts = start_date_ts.tz_localize(None)
            if hasattr(end_date_ts, 'tz_localize'):
                end_date_ts = end_date_ts.tz_localize(None)
        # O alternativamente, si los timestamps no tienen zona horaria pero los datos sí
        elif start_date_ts is not None and end_date_ts is not None:
            # Asegurar que los timestamps para comparación sean naive
            if hasattr(start_date_ts, 'tz') and start_date_ts.tz is not None:
                start_date_ts = start_date_ts.tz_localize(None)
            if hasattr(end_date_ts, 'tz') and end_date_ts.tz is not None:
                end_date_ts = end_date_ts.tz_localize(None)
        
        # Ahora es seguro aplicar el filtro
        filtered_data = filtered_data[
            (filtered_data['fecha_factura'] >= start_date_ts) &
            (filtered_data['fecha_factura'] <= end_date_ts)
        ]
    
    # Aplicar filtro de obras
    if 'obra' in filtered_data.columns and selected_obras:
        filtered_data = filtered_data[filtered_data['obra'].astype(str).isin(map(str, selected_obras))]
    
    # Aplicar filtro de subcategorías
    if 'subcategoria' in filtered_data.columns and selected_subcategorias:
        filtered_data = filtered_data[filtered_data['subcategoria'].astype(str).isin(map(str, selected_subcategorias))]
else:
    filtered_data = pd.DataFrame()


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
                # Título según si tenemos datos de obra o no
                title = "Total por Categoría y Obra" if has_obra else "Total por Categoría"
                st.subheader(title)
                
                # Preparar datos para el gráfico
                if has_obra:
                    # Agrupar por categoría y obra
                    bar_chart_data = filtered_data.groupby(['categoria_id', 'obra'])['total'].sum().reset_index()
                    color_by = 'obra'
                    barmode = 'group'  # Barras agrupadas (no apiladas)
                else:
                    # Solo agrupar por categoría
                    bar_chart_data = filtered_data.groupby('categoria_id')['total'].sum().reset_index()
                    color_by = 'categoria_id'
                    barmode = 'relative'
                
                # Crear gráfico si hay datos
                if not bar_chart_data.empty:
                    # Crear gráfico de barras
                    fig_bar = px.bar(
                        bar_chart_data,
                        x='categoria_id',
                        y='total',
                        color=color_by,
                        barmode=barmode,
                        title=title,
                        labels={'categoria_id': 'Categoría', 'total': 'Total', 'obra': 'Obra'},
                    )
                    # Ajustar el diseño para mejorar la visualización
                    fig_bar.update_layout(
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        margin=dict(t=100)  # Mayor margen superior para la leyenda
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

# # Add help information
# st.markdown("---")
# with st.expander(":information_source: Ayuda para la Visualización de Datos"):
#     st.markdown("""
#     ### Consejos para usar esta página:
    
#     - **Filtros de datos**: Utiliza los controles del sidebar para filtrar los datos por categoría, fecha y obras.
#     - **Gráficos interactivos**: Puedes interactuar con los gráficos haciendo clic en las leyendas o elementos.
#     - **Estadísticas**: Explora las diferentes visualizaciones para obtener insights sobre los datos.
#     - **Análisis por categoría**: Revisa las tendencias por categoría y subcategoría para identificar patrones.
    
#     Para obtener más ayuda, contacta al administrador del sistema.
#     """)
