import streamlit as st
import pandas as pd
import os
import uuid
import time
from utils.authentication import Authentication
from sql_agent import run_sql_agent # <--- Added
from conversation_handler import determine_conversation_intent, generate_conversational_response # <--- Added for conversational abilities
from langchain_core.messages import HumanMessage, AIMessage # <--- Added
from pages.utils_3 import get_data_loader_instance
from utils.config import get_config
from supabase import create_client, Client


# Cargar estilos personalizados si existen
try:
    with open(os.path.join("assets", "styles.css")) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
except Exception as e:
    st.warning(f"Unable to load custom CSS: {e}")

# Autenticación
authentication = Authentication()
if not authentication.check_authentication():
    st.stop()


# Inicialización de la sesión
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
    print(f"Created new session ID: {st.session_state.session_id}")

# Asegurar que tenemos un ID de usuario válido para las conversaciones
if "user_id" not in st.session_state:
    # Obtener el ID de usuario de la sesión (establecido durante el login)
    # Si no existe, usa un valor anónimo con un UUID temporal
    user_id = st.session_state.get("user_id", f"anonymous_{uuid.uuid4()}")
    st.session_state.user_id = user_id
    print(f"Set user_id to: {user_id}")

# Asegurar que tenemos un diccionario de conversaciones por usuario
if "user_conversations" not in st.session_state:
    st.session_state.user_conversations = {}
    
# Inicializar mensajes para el usuario actual si no existen
if st.session_state.user_id not in st.session_state.user_conversations:
    st.session_state.user_conversations[st.session_state.user_id] = []
    
# Vincular los mensajes del usuario actual a la sesión de mensajes actual
if "messages" not in st.session_state:
    # Primera inicialización - crear con mensaje de bienvenida
    st.session_state.messages = st.session_state.user_conversations[st.session_state.user_id]
    if len(st.session_state.messages) == 0:
        # Añadir mensaje de bienvenida con el nombre del usuario
        user_name = st.session_state.get("name", "Usuario")
        welcome_message = {
            "role": "assistant",
            "content": f"👋 Hola {user_name}, soy tu asistente SQL. Puedes preguntarme sobre las facturas de Palma Terra y te ayudaré a obtener la información que necesitas. ¿En qué puedo ayudarte hoy?",
            "dataframe": None,
            "is_welcome": True
        }
        st.session_state.messages.append(welcome_message)
else:
    # Asegurar que siempre usamos los mensajes del usuario actual
    st.session_state.messages = st.session_state.user_conversations[st.session_state.user_id]

# Al inicio de 1_SQL_Chatbot.py
if 'active_feedback_message_id' not in st.session_state:
    st.session_state.active_feedback_message_id = None
if 'feedback_texts' not in st.session_state:
    st.session_state.feedback_texts = {}
if 'feedback_given' not in st.session_state: # Ya deberías tener esta
    st.session_state.feedback_given = set()
if 'conversation_stats' not in st.session_state:
    # Añadimos estadísticas conversacionales para análisis
    st.session_state.conversation_stats = {
        "sql_queries": 0,
        "conversational_responses": 0,
        "last_intent_type": None
    }


# Interfaz principal del Chat SQL
st.title("Consulta Inteligente de la Base de Datos de Facturación")
st.markdown("Asistente de Inteligencia Artificial Potenciado por **LangGraph y GPT-4**. :speech_balloon: :robot_face:", help="Utiliza la ayuda en la barra lateral para obtener más información")

# Opciones y filtros en la barra lateral
with st.sidebar:
    with st.expander(":gear: Ayuda"):
        st.info("""
        :page_facing_up: **Asistente Inteligente de Consultas SQL**
        
        Realice consultas en lenguaje natural sobre su base de datos ('portal_desglosado' table).
        El sistema convertirá su pregunta en SQL, la ejecutará y le mostrará los resultados.
        
        **Ejemplos de preguntas que puede hacer:**
        
        - ¿Cuánto ha sido el total gastado en la obra 'K. Las Vias'?
        - ¿Cuáles son las facturas de viguetas en la obra 'K. Aguamarina' que están pagadas?
        - ¿Qué obras han gastado más de 100,000 en la categoría 'SUBCONTRATO'?
        """)
        
    st.subheader(":gear: Filtros de Datos")
    st.markdown("Estos filtros se aplicarán automáticamente a tus consultas:")

    # --- Importar las funciones centralizadas ---
    from utils.chatbot_supabase import init_chatbot_supabase_client, get_chatbot_filter_options

    # --- Usar las funciones centralizadas con caché ---
    supabase_client_chatbot = init_chatbot_supabase_client()

    if supabase_client_chatbot:
        chatbot_filter_opts = get_chatbot_filter_options(supabase_client_chatbot)
    else:
        # Fallback to empty options if Supabase client failed
        chatbot_filter_opts = {key: [] for key in ['obras', 'proveedores', 'subcategorias', 'categorias']}
        st.error("No se pudo conectar a Supabase. Los filtros no estarán disponibles.")

    # --- Multiselect Filters using cached options ---
    obras_seleccionadas = st.multiselect(
        ":building_construction: Obra",
        options=chatbot_filter_opts['obras'],
        default=[],
        key="obra_filter",
        help="Seleccione una o más obras. Si no selecciona ninguna, se considerarán todas."
    )

    proveedores_seleccionados = st.multiselect(
        ":busts_in_silhouette: Proveedor",
        options=chatbot_filter_opts['proveedores'],
        default=[],
        key="proveedor_filter",
        help="Seleccione uno o más proveedores. Si no selecciona ninguno, se considerarán todos."
    )

    subcategorias_seleccionados = st.multiselect(
        ":bookmark_tabs: Subcategorías", # Changed icon for variety
        options=chatbot_filter_opts['subcategorias'],
        default=[],
        key="subcategoria_filter",
        help="Seleccione una o más subcategorías. Si no selecciona ninguna, se considerarán todas."
    )

    categorias_seleccionadas = st.multiselect(
        ":label: Categoría",
        options=chatbot_filter_opts['categorias'],
        default=[],
        key="categoria_filter",
        help="Seleccione una o más categorías. Si no selecciona ninguna, se considerarán todas."
    )

    st.divider()
    
    if st.button("Limpiar conversación", use_container_width=True):
        st.session_state.messages = []
        # st.session_state.conversation_initialized = False # Ya no es necesaria esta bandera
        # También limpiaremos la conversación del usuario actual en user_conversations
        if st.session_state.user_id in st.session_state.user_conversations:
            st.session_state.user_conversations[st.session_state.user_id] = []
        # Also clear any agent-specific state if necessary, e.g., chat_history for the agent
        st.rerun()

# Inicializar el historial de chat y el seguimiento de feedback si no existe
if "messages" not in st.session_state:
    st.session_state.messages = [] # This will store dicts: {"role": "user/assistant", "content": "...", "dataframe": pd.DataFrame (optional)}
    st.session_state.conversation_initialized = False
    
# Inicializar seguimiento de feedback si no existe
if "feedback_given" not in st.session_state:
    st.session_state.feedback_given = set() # Conjunto de IDs de mensajes que ya recibieron feedback


# Añadir sistema de feedback
def update_feedback(last_query, rating, text=None):
    try:
        from sql_agent import log_chat_interaction
        
        # Solo necesitamos actualizar los campos de feedback
        log_chat_interaction(
            session_id=st.session_state.session_id,
            user_id=st.session_state.user_id,
            user_input=last_query,
            corrected_entities={},  # No es necesario para la actualización
            generated_sql="",       # No es necesario para la actualización
            query_type="",          # No es necesario para la actualización
            sql_error="",           # No es necesario para la actualización
            execution_success=True, # No es necesario para la actualización
            response_time_ms=0,     # No es necesario para la actualización
            feedback_rating=rating,
            feedback_text=text
        )
        return True
    except Exception as e:
        st.error(f"Error al guardar feedback: {e}")
        return False
        
# Mostrar mensajes en el chat
for i, message in enumerate(st.session_state.messages):
    # Generar un ID único para este mensaje basado en su posición y contenido
    # Usar el contenido y el rol para mayor unicidad, y la posición para orden
    message_id = f"msg_{i}_{message['role']}_{hash(message['content'][:50])}"

    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "dataframe" in message and message["dataframe"] is not None and not message["dataframe"].empty:
            with st.expander("Ver datos completos", expanded=False):
                st.dataframe(message["dataframe"], use_container_width=True)
        
        # Mostrar botones de feedback solo para mensajes del asistente que aún no han recibido feedback
        # y que no sean el mensaje de bienvenida inicial.
        # Determinar si es el mensaje de bienvenida para no mostrar feedback
        # Usamos la clave 'is_welcome' que se añade al mensaje de bienvenida personalizado
        is_welcome_message = message.get("is_welcome", False)
        if message["role"] == "assistant" and message_id not in st.session_state.feedback_given and not is_welcome_message:
            # Encontrar el mensaje del usuario anterior a este mensaje del asistente (para el registro)
            user_query = ""
            if i > 0 and st.session_state.messages[i-1]["role"] == "user":
                user_query = st.session_state.messages[i-1]["content"]
            
            with st.expander("¿Fue útil esta respuesta?", expanded=False):
                col1, col2, col3 = st.columns(3)
                
                if col1.button("👍 Útil", key=f"useful_{message_id}"):
                    if update_feedback(user_query, 5, "Útil"):
                        st.session_state.feedback_given.add(message_id)
                        st.success("¡Gracias por tu feedback!")
                        st.rerun()  # Recargar para quitar los botones
                
                if col2.button("😐 Regular", key=f"ok_{message_id}"):
                    if update_feedback(user_query, 3, "Regular"):
                        st.session_state.feedback_given.add(message_id)
                        st.success("¡Gracias por tu feedback!")
                        st.rerun()  # Recargar para quitar los botones
                
                # Lógica para el botón '👎 No útil' y su campo de texto
                if col3.button("👎 No útil", key=f"not_useful_{message_id}"):
                    if st.session_state.get('active_feedback_message_id') == message_id:
                        st.session_state.active_feedback_message_id = None  # Ocultar si ya está activo
                    else:
                        # Mostrar campo de texto para feedback detallado
                        st.session_state.active_feedback_message_id = message_id
                        # Inicializar texto de feedback para este mensaje si no existe
                        if message_id not in st.session_state.feedback_texts:
                            st.session_state.feedback_texts[message_id] = ""
                            
                        # Mostrar campo de texto y botón de envío
                        feedback_text = st.text_area(
                            "¿En qué podemos mejorar?", 
                            value=st.session_state.feedback_texts.get(message_id, ""),
                            key=f"feedback_text_{message_id}",
                            on_change=lambda: setattr(st.session_state.feedback_texts, message_id, st.session_state[f"feedback_text_{message_id}"])
                        )
                        
                        if st.button("Enviar feedback", key=f"submit_feedback_{message_id}"):
                            if update_feedback(user_query, 1, feedback_text):  # 1 = No útil
                                st.session_state.feedback_given.add(message_id)
                                st.session_state.active_feedback_message_id = None
                                st.success("¡Gracias por tu feedback detallado!")
                                st.rerun()  # Recargar para quitar los botones


                if st.session_state.get('active_feedback_message_id') == message_id:
                    # Asegurar que existe una entrada para este message_id en feedback_texts
                    if message_id not in st.session_state.feedback_texts:
                        st.session_state.feedback_texts[message_id] = ""

                    current_feedback_text_value = st.text_input(
                        "¿Puedes decirnos qué falló?",
                        value=st.session_state.feedback_texts.get(message_id, ""),
                        key=f"feedback_text_input_{message_id}"
                    )
                    # Persistir cualquier cambio del text_input en session_state inmediatamente
                    st.session_state.feedback_texts[message_id] = current_feedback_text_value

                    if st.button("Enviar comentario", key=f"send_feedback_button_{message_id}"):
                        final_feedback_text = st.session_state.feedback_texts.get(message_id, "")
                        if final_feedback_text:  # Comprobar si el texto no está vacío
                            if update_feedback(user_query, 1, final_feedback_text):
                                st.session_state.feedback_given.add(message_id)
                                st.success("¡Gracias por tu feedback detallado!")
                                st.session_state.active_feedback_message_id = None  # Ocultar campo de texto
                                st.session_state.feedback_texts[message_id] = ""  # Limpiar texto guardado
                                st.rerun()
                            else:
                                st.error("No se pudo enviar el feedback. Inténtalo de nuevo.")
                        else:
                            st.warning("Por favor, escribe un comentario antes de enviar.")


# Función para procesar la consulta del usuario con el agente SQL
def process_query_with_agent(user_input_text: str, current_streamlit_filters: dict):
    # Convert st.session_state.messages to LangChain BaseMessages
    chat_history_for_agent = []
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            chat_history_for_agent.append(HumanMessage(content=msg["content"]))
        elif msg["role"] == "assistant":
            chat_history_for_agent.append(AIMessage(content=msg["content"]))
    
    # Ensure the latest user message isn't duplicated in history passed to agent
    # The agent itself will add the current user_input to its history.
    # So, pass history *before* the current user_input.
    
    with st.spinner(":thinking_face: Analizando tu consulta..."):
        try:
            # NUEVA FUNCIONALIDAD: Determinar si necesitamos ejecutar SQL o podemos responder conversacionalmente
            intent_data = determine_conversation_intent(user_input_text, chat_history_for_agent)
            
            # Registrar intención para propósitos de análisis/depuración
            print(f"Intent classification: {intent_data}")
            
            # Si necesitamos una respuesta conversacional sin SQL
            if not intent_data.get("needs_sql", True):
                    # Mostrar indicador sutil del modo conversacional (opcional)
                    st.toast(f"Modo conversacional: {intent_data['intent_type']}", icon="💬")
                    
                    # Generar respuesta conversacional
                    # Mantener lista de resultados SQL previos en session_state
                    if "sql_results_dfs" not in st.session_state:
                        st.session_state["sql_results_dfs"] = []
                    # Si hay un nuevo results_df, agregarlo a la lista
                    if 'response_df' in locals() and response_df is not None and not response_df.empty:
                        st.session_state["sql_results_dfs"].append(response_df)
                    # Serializar todos los DFs a JSON (muestra de hasta 50 filas por DF)
                    sql_results_json = "\n\n".join([
                        df.head(50).to_json(orient="records", date_format="iso", force_ascii=False)
                        for df in st.session_state["sql_results_dfs"]
                    ])
                    response_data = generate_conversational_response(
                        user_input=user_input_text,
                        chat_history=chat_history_for_agent,
                        intent_data=intent_data,
                        sql_results_json=sql_results_json
                    )
                    
                    # Actualizar historial de chat con la respuesta conversacional
                    assistant_message = {
                        "role": "assistant",
                        "content": response_data.get("natural_response", "No se pudo generar una respuesta conversacional."),
                    }
                    
                    st.session_state.messages.append(assistant_message)
                    return  # No continuamos con el procesamiento SQL
            
            # Si necesitamos SQL, continuamos con el flujo original
            # Prepare filters for the agent
            # The agent expects a Dict[str, List[str]]
            # Ensure selected filters are passed correctly
            agent_filters = {}
            if current_streamlit_filters.get("obras_seleccionadas"):
                agent_filters["obra"] = current_streamlit_filters["obras_seleccionadas"]
            if current_streamlit_filters.get("proveedores_seleccionados"):
                agent_filters["proveedor"] = current_streamlit_filters["proveedores_seleccionados"]
            if current_streamlit_filters.get("subcategorias_seleccionados"):
                agent_filters["subcategoria"] = current_streamlit_filters["subcategorias_seleccionados"]

            agent_response_data = run_sql_agent(
                user_input=user_input_text,
                chat_history=chat_history_for_agent,
                streamlit_filters=agent_filters, # These are the *selected* filters
                obras_disponibles=chatbot_filter_opts['obras'], # Pass the *full list* of available options
                proveedores_disponibles=chatbot_filter_opts['proveedores'], # Pass the *full list*
                subcategorias_disponibles=chatbot_filter_opts['subcategorias'], # Pass the *full list*
                # categoria_disponibles=chatbot_filter_opts['categorias'], # Add if your agent uses this
                session_id=st.session_state.session_id,
                user_id=st.session_state.user_id
            )
            
            # The agent's run_sql_agent should return the full updated chat history
            # For now, we'll just append its natural response.
            # A more robust way is for run_sql_agent to return the new AIMessage object(s).
            
            response_content = agent_response_data.get("natural_response", "No se pudo obtener respuesta del agente.")
            response_df = agent_response_data.get("results_df")

            # Update Streamlit's session state messages with the agent's response
            assistant_message = {
                "role": "assistant",
                "content": response_content,
            }
            if response_df is not None and not response_df.empty:
                assistant_message["dataframe"] = response_df
            print(response_df)
            import tiktoken
            import json
            encoding = tiktoken.encoding_for_model("gpt-4o")
            # Tokens del DataFrame como string
            df_str = str(response_df)
            tokens_str = len(encoding.encode(df_str))
            print(f"Tokens en str(response_df): {tokens_str}")
            # Tokens del DataFrame como JSON
            df_json = response_df.to_json(orient="records", force_ascii=False)
            tokens_json = len(encoding.encode(df_json))
            print(f"Tokens en response_df.to_json: {tokens_json}")
            # Si quieres ver el JSON también:
            # print(df_json)
            st.session_state.messages.append(assistant_message)
            
            # The agent's returned chat_history should ideally be used to sync st.session_state.messages
            # For simplicity here, we're managing st.session_state.messages directly based on agent output.

        except Exception as e:
            st.error(f"Error al procesar la consulta: {e}")
            error_message = {"role": "assistant", "content": f"Lo siento, ocurrió un error: {e}"}
            st.session_state.messages.append(error_message)


# Procesar entrada del usuario
if prompt := st.chat_input("Haz una pregunta sobre las facturas referente a las obras de Palma Terra..."):
    # Agregar mensaje del usuario al historial de Streamlit
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Mostrar mensaje del usuario en la UI
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Preparar filtros actuales para el agente
    current_filters = {
        "obras_seleccionadas": obras_seleccionadas,
        "proveedores_seleccionados": proveedores_seleccionados,
        "subcategorias_disponibles": subcategorias_seleccionados
    }
    
    # Procesar la consulta con el agente (esto también actualiza st.session_state.messages)
    process_query_with_agent(prompt, current_filters)

    # Re-render the chat to show the new assistant message
    # This happens automatically as st.session_state.messages was modified
    # and Streamlit reruns on chat_input.
    # We might need st.rerun() if updates are not appearing immediately after process_query_with_agent.
    st.rerun() # Ensure UI updates with the new message
