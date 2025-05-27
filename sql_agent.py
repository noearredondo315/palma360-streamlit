import os
import uuid
import pandas as pd
import time
from typing import TypedDict, List, Dict, Optional, Annotated

from langchain_community.utilities import SQLDatabase
import json
import ast
import concurrent.futures
from langchain_core.output_parsers import StrOutputParser
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.output_parsers.string import StrOutputParser
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langgraph.graph import StateGraph, END
# from langgraph.checkpoint.sqlite import SqliteSaver # For persistence if needed later
from sqlalchemy import create_engine
import streamlit as st # For st.secrets
from pydantic import BaseModel, Field # Added for structured output
from openai import OpenAI # Added for structured output client

# --- Configuration & Initialization ---

# Load secrets (ensure this is called within Streamlit context or handled appropriately)
def get_supabase_credentials():
    return {
        "user": st.secrets["supabase"]["user"],
        "password": st.secrets["supabase"]["password"],
        "host": st.secrets["supabase"]["host"],
        "port": st.secrets["supabase"]["port"],
        "dbname": st.secrets["supabase"]["dbname"]
    }

def get_db_engine():
    creds = get_supabase_credentials()
    db_url = f"postgresql+psycopg2://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['dbname']}"
    return create_engine(db_url)

def get_sql_database_tool(include_tables: List[str] = ['portal_desglosado']):
    engine = get_db_engine()
    return SQLDatabase(engine, include_tables=include_tables)

# --- Chat Analytics Function ---
def log_chat_interaction(
    session_id: str,
    user_id: str,
    user_input: str,
    corrected_entities: dict,
    generated_sql: str,
    query_type: str,
    sql_error: str,
    execution_success: bool,
    response_time_ms: int,
    feedback_rating: int = None,
    feedback_text: str = None
):
    """Almacena la interacción del chat en la tabla de análisis de Supabase"""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            from sqlalchemy.sql import text
            # Crear consulta parametrizada usando text() de SQLAlchemy
            query = text("""
                INSERT INTO chat_analytics 
                (session_id, user_id, user_input, corrected_entities, generated_sql, 
                query_type, sql_error, execution_success, response_time_ms, 
                feedback_rating, feedback_text)
                VALUES (:session_id, :user_id, :user_input, :corrected_entities, :generated_sql, 
                :query_type, :sql_error, :execution_success, :response_time_ms, 
                :feedback_rating, :feedback_text)
            """)
            
            # Crear diccionario de parámetros
            params = {
                "session_id": session_id, 
                "user_id": user_id, 
                "user_input": user_input, 
                "corrected_entities": json.dumps(corrected_entities), 
                "generated_sql": generated_sql, 
                "query_type": query_type, 
                "sql_error": sql_error, 
                "execution_success": execution_success, 
                "response_time_ms": response_time_ms, 
                "feedback_rating": feedback_rating, 
                "feedback_text": feedback_text
            }
            
            # Ejecutar la consulta con los parámetros
            conn.execute(query, params)
            conn.commit()
            
        print(f"Successfully logged chat interaction for session {session_id}")
    except Exception as e:
        print(f"Error logging chat interaction: {e}")
        import traceback
        traceback.print_exc()

# Initialize LLM and Embeddings (ensure OPENAI_API_KEY is set in environment or st.secrets)
openai_api_key = st.secrets["openai"]["OPENAI_API_KEY"]
# print (openai_api_key) # Optional: remove or comment out print
llm = ChatOpenAI(model="gpt-4.1-mini-2025-04-14", temperature=0, api_key=openai_api_key) # Or your preferred model
embeddings_model = OpenAIEmbeddings(model="text-embedding-3-small", api_key=openai_api_key)
client = OpenAI(api_key=openai_api_key) # Initialize OpenAI client for structured outputs

# Pydantic model for structured output (as per user example)
class Result(BaseModel):
    obra: Optional[str] = Field(None, description="The identified project name from the available list.")
    proveedor: Optional[str] = Field(None, description="The identified supplier name from the available list.")

# --- Agent State Definition ---
class AgentState(TypedDict):
    user_input: str
    chat_history: List[BaseMessage]
    table_schema: str
    extracted_entities: dict  # May hold initial raw, or be merged into corrected_entities
    corrected_entities: dict  # Entities after structured output and other processing
    streamlit_filters: dict
    query_type: Optional[str]  # 'STATIC' or 'SEMANTIC'
    sql_query: Optional[str]
    results_df: Optional[pd.DataFrame]
    natural_response: Optional[str]
    sql_error: Optional[str]
    clarification_question: Optional[str]
    obras_disponibles: List[str]      # New: List of available obras
    proveedores_disponibles: List[str] # New: List of available proveedores
    subcategorias_disponibles: List[str] # New: List of available subcategorias

# --- Node Functions ---

def process_input_and_extract_entities_node(state: AgentState):
    print("--- Running: process_input_and_extract_entities_node (Structured Output Version) ---")
    user_input = state["user_input"]
    obras_disponibles = state.get("obras_disponibles", [])
    proveedores_disponibles = state.get("proveedores_disponibles", [])

    # client is initialized globally
    input_data = {"proveedores": proveedores_disponibles, "obras": obras_disponibles}
    
    current_corrected_entities = {}
    
    # --- Structured Output Entity Extraction for Obra and Proveedor ---
    try:
        completion = client.beta.chat.completions.parse(
            model="gpt-4.1-mini", # As per user's example
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Tu tarea es identificar forzosamente el nombre correcto de la obra y del proveedor "
                        "a partir del texto del usuario, aunque estos estén mal escritos o incompletos. "
                        f"Para ello, deberás hacer un mapeo preciso usando como referencia los datos contenidos en {input_data}. "
                        "Devuelve únicamente los valores exactos y completos de obra y proveedor encontrados en la lista de obras y proveedores, "
                        "basándote en la mejor coincidencia semántica. Si una entidad no se menciona o no se puede mapear con certeza a la lista, omítela (devuelve null o no incluyas la clave en el JSON resultante)."
                    )
                },
                {
                    "role": "user",
                    "content": user_input
                },
            ],
            response_format=Result, # Using the Pydantic model 'Result' defined globally
        )
        parsed_result = completion.choices[0].message.parsed
        
        if parsed_result:
            if parsed_result.obra:
                current_corrected_entities["obra"] = parsed_result.obra
            if parsed_result.proveedor:
                current_corrected_entities["proveedor"] = parsed_result.proveedor
        print(f"Structured Output - Identified Obra/Proveedor: {current_corrected_entities}")

    except Exception as e:
        print(f"Structured Output - Error during obra/proveedor extraction: {e}")
        # Fallback: if structured output fails, these entities might remain empty or could use raw input as a last resort.

    # --- Existing Description Extraction (Kept separate as per analysis) ---
    # This part handles 'descripcion' if it's not part of the structured output for obra/proveedor.
    entity_extraction_prompt_desc = ChatPromptTemplate.from_messages([
        ("system", """
        You are an expert at extracting key entities from a user's question about expenses and construction projects.
        Given the user's question, identify values for:
        - 'descripcion' (description of materials, services, or items the user is searching for)
        
        Return the extracted entities as a JSON object. If an entity is not mentioned, do not include it in the JSON.
        For 'descripcion', extract key phrases that describe what the user is looking for (materials, items, services).
        
        Example:
        User question: ¿Se han pagado facturas de pasajuntas o similares de herrería en la obra Bodega Acatlán E2?
        JSON: {{""descripcion"": ""pasajuntas o similares de herrería""}}
        """),
        ("human", "{user_question}")
    ])
    # llm and StrOutputParser are defined globally
    entity_extractor_chain_desc = entity_extraction_prompt_desc | llm | StrOutputParser()
    
    try:
        extracted_desc_str = entity_extractor_chain_desc.invoke({"user_question": user_input})
        print(f"LLM - Extracted descripcion string: {extracted_desc_str}")
        if extracted_desc_str.strip():
            extracted_desc_json = json.loads(extracted_desc_str)
            if "descripcion" in extracted_desc_json:
                current_corrected_entities["descripcion"] = extracted_desc_json["descripcion"]
                print(f"LLM - Extracted descripcion: {current_corrected_entities['descripcion']}")
    except json.JSONDecodeError:
        print(f"LLM - Failed to decode JSON from descripcion extraction: {extracted_desc_str}")
    except Exception as e:
        print(f"LLM - Error during 'descripcion' extraction: {e}")
            
    print(f"Final combined corrected entities: {current_corrected_entities}")
    
    # Get table schema (this part is still relevant)
    db = get_sql_database_tool()
    table_schema = db.get_table_info()

    return {
        "extracted_entities": current_corrected_entities, # Using this to store all found entities
        "corrected_entities": current_corrected_entities, # This now holds structured output + descripcion
        "table_schema": table_schema
    }

def generate_sql_query_node(state: AgentState):
    print("--- Running: generate_sql_query_node ---")
    TABLE_NAME = "portal_desglosado"
    subcategoria = state["subcategorias_disponibles"]

    # Define column descriptions for better context
    descripciones_columnas = {
        "obra": "Nombre del proyecto de construcción",
        "folio": "Número de factura o identificador único",
        "fecha_factura": "Fecha en que se emitió la factura (formato YYYY-MM-DD)",
        "cantidad": "Cantidad del producto o material adquirido",
        "tipo_gasto": "Indica si la compra es costo directo, garantía o servicio.",
        "subtotal": "Monto antes de impuestos",
        "total": "Monto total de la factura incluyendo impuestos",
        "descripcion": "Descripción general del producto que NO debe usarse para búsquedas de categorías o materiales específicos",
        "categoria_id": """
            [IMPORTANTE] Agrupación macro de los costos. Los valores posibles incluyen:
            ACERO, ANDAMIOS, CIMBRA, ESCALERA Y PUNTALES, FERRETERÍA, HERRERÍA, AUXILIARES, 
            CAMIONES, EQUIPO MENOR, INSTALACIONES, MAQUINARIA, SUBCONTRATO, OTROS GASTOS    .
            Úsalo para filtros de alto nivel o para resumir el gasto por rubro.
        """,
        "subcategoria": """
            [MUY IMPORTANTE] Clasificación específica del material o servicio.
            AQUÍ ES DONDE SE DEBE BUSCAR MATERIALES ESPECÍFICOS como CEMENTO, VARILLA, ARENA, GRAVA.
            NUNCA busques materiales en 'descripcion', SIEMPRE en 'subcategoria'.
            Ejemplos de valores: {subcategoria}
        """,
        "proveedor": "Nombre del proveedor que emitió la factura",
        "residente": "Persona responsable del proyecto",
        "estatus": "Estado actual de la factura (ej. Pagado, Proceso de pago)",
        "moneda": "Tipo de moneda (ej. MXN, USD)",
        "precio_unitario": "Precio por unidad del material o servicio",
        "unidad": "Unidad de medida (ej. kg, m3, pieza)"
    }

    column_descriptions_text = "\n".join([f'- "{col}": {desc}' for col, desc in descripciones_columnas.items()])

    prompt_template = ChatPromptTemplate.from_messages([
        ("system", f"""
        Eres un asistente experto en PostgreSQL que responde en ESPAÑOL. Dada una pregunta del usuario, historial de chat, 
        esquema de tabla, filtros de UI de Streamlit y entidades corregidas, genera una consulta PostgreSQL sintácticamente correcta. 
        Prioriza los filtros de Streamlit si se proporcionan. 
        El nombre de la tabla es '{TABLE_NAME}'.

        Instrucciones Importantes:
        1.  Tu ÚNICA salida DEBE SER la consulta SQL. No incluyas NINGÚN texto adicional, explicaciones, saludos o formato markdown.
        2.  Siempre responde en ESPAÑOL cuando se te pida una respuesta en lenguaje natural (en otros nodos), pero para ESTE PASO, solo genera SQL.
        3.  Utiliza la columna "subcategoria" para buscar materiales o servicios específicos (ej. CEMENTO, VARILLA, ARENA). NO uses la columna "descripcion" para esto.
        4.  Presta atención a las descripciones de las columnas para entender mejor su significado.

        Esquema de la Tabla ('{TABLE_NAME}'):
        {{table_schema}}  # Escaped curly braces for f-string

        Descripciones Detalladas de las Columnas:
        {column_descriptions_text}

        Filtros de Streamlit (aplícalos usando cláusulas WHERE):
        {{streamlit_filters}} # Escaped curly braces for f-string

        Entidades Corregidas Proporcionadas:
        {{corrected_entities}}

        Instrucciones para Entidades Corregidas:
        - Si una entidad (como 'obra' o 'proveedor') está presente en 'Entidades Corregidas Proporcionadas',
          DEBES usar el valor exacto proporcionado para esa entidad en tu cláusula WHERE con un operador de igualdad (ej., "proveedor" = 'VALOR_CORREGIDO_EXACTO').
        - NO uses cláusulas LIKE para entidades que han sido corregidas y proporcionadas aquí. Usa el valor exacto.
        - Estas entidades corregidas tienen prioridad sobre las menciones originales en la pregunta del usuario para los campos correspondientes.
        
        Asegúrate de usar la tabla '{TABLE_NAME}'. Si el usuario pregunta por algo que no está en el esquema o descripciones, 
        indica amablemente que solo puedes responder preguntas sobre la tabla '{TABLE_NAME}'.
        Si hay un error SQL previo, intenta corregirlo basándote en el mensaje de error.
        Error SQL Previo (si existe): {{sql_error}} # Escaped curly braces for f-string

        Ejemplos de Consultas (adapta según sea necesario):

        1.  **Pregunta:** ¿Cuánto ha sido el total gastado en la obra 'K. Las Vias'?
            **SQL:** `SELECT SUM("total") AS total_gastado FROM {TABLE_NAME} WHERE "obra" = 'K. Las Vias';`

        2.  **Pregunta:** ¿Cuánto ha gastado el residente 'Arq. Martin' en yeso por obra?
            **SQL:** `SELECT "obra", SUM("subtotal") AS subtotal_gastado FROM {TABLE_NAME} WHERE "residente" = 'Arq. Martin' AND "subcategoria" = 'YESO' GROUP BY "obra" ORDER BY subtotal_gastado DESC;`

        3.  **Pregunta:** ¿Cuál es el gasto promedio por factura en 'INSTALACIONES' por obra?
            **SQL:** `SELECT "obra", AVG("total") AS gasto_promedio FROM {TABLE_NAME} WHERE "categoria_id" = 'INSTALACIONES' GROUP BY "obra" ORDER BY gasto_promedio DESC;`

        4.  **Pregunta:** ¿Qué obras tienen facturas de suministro de agua y cuánto gastaron?
            **SQL:** `SELECT "obra", SUM("total") AS total_gastado FROM {TABLE_NAME} WHERE "subcategoria" = 'SUMINISTRO AGUA' GROUP BY "obra" ORDER BY total_gastado DESC;`

        5.  **Pregunta:** ¿Cuál es el proveedor con mayor gasto en 'SUBCONTRATO'?
            **SQL:** `SELECT "proveedor", SUM("total") AS total_gastado FROM {TABLE_NAME} WHERE "categoria_id" = 'SUBCONTRATO' GROUP BY "proveedor" ORDER BY total_gastado DESC LIMIT 1;`

        6.  **Pregunta:** ¿Cuántas facturas hemos recibido del proveedor 'CEMEX'?
            **SQL:** `SELECT COUNT(*) AS numero_facturas FROM {TABLE_NAME} WHERE "proveedor" = 'CEMEX';`

        7.  **Pregunta:** ¿Cuál es la factura más cara (mayor total) en la categoría 'MAQUINARIA'?
            **SQL:** `SELECT "folio", "total" FROM {TABLE_NAME} WHERE "categoria_id" = 'MAQUINARIA' ORDER BY "total" DESC LIMIT 1;`

        8.  **Pregunta:** ¿Cuánto gastamos en 'LIMPIEZA' durante el mes de marzo de 2024?
            **SQL:** `SELECT SUM("total") AS total_gastado FROM {TABLE_NAME} WHERE "categoria_id" = 'LIMPIEZA' AND "fecha" >= '2024-03-01' AND "fecha" <= '2024-03-31';`

        9.  **Pregunta:** ¿Cuántas facturas ha gestionado cada residente?
            **SQL:** `SELECT "residente", COUNT(*) AS numero_facturas FROM {TABLE_NAME} GROUP BY "residente" ORDER BY numero_facturas DESC;`

        10. **Pregunta:** ¿Cuál es el gasto total por obra y por categoría?
            **SQL:** `SELECT "obra", "categoria_id", SUM("total") AS total_gastado FROM {TABLE_NAME} GROUP BY "obra", "categoria_id" ORDER BY "obra", total_gastado DESC;`

        11. **Pregunta:** ¿Cuál es el **gasto total** en **cemento** considerando únicamente las obras 'Bodega Acatlán E2', 'K. Las Vias' y 'Bosques de la Cantera'?
            **SQL:** `SELECT SUM("total") AS total_cemento FROM {TABLE_NAME} WHERE "subcategoria" = 'CEMENTO' AND "obra" IN ('Bodega Acatlán E2', 'K. Las Vias', 'Bosques de la Cantera');`

        12. **Pregunta:** ¿**Cuántas facturas** de **cemento** tenemos para **cada una** de las obras 'Torre Insurgentes', 'Casa Higuera Altozano' y 'Casa ZDT'?
            **SQL:** `SELECT "obra", COUNT(*) AS numero_facturas_cemento FROM {TABLE_NAME} WHERE "subcategoria" = 'CEMENTO' AND "obra" IN ('Torre Insurgentes', 'Casa Higuera Altozano', 'Casa ZDT') GROUP BY "obra";`

        13. **Pregunta:** ¿Cuál es el **costo promedio** por factura de **cemento** si solo vemos las obras 'Reparacion Casa Club Altozano' y 'Muro Altozano'?
            **SQL:** `SELECT AVG("total") AS promedio_cemento FROM {TABLE_NAME} WHERE "subcategoria" = 'CEMENTO' AND "obra" IN ('Reparacion Casa Club Altozano', 'Muro Altozano');`

        14. **Pregunta:** Muéstrame el **gasto total** en **cemento** para 'Bodega Acatlán E2' y 'K. Las Vias', pero **desglosado por proveedor**.
            **SQL:** `SELECT "proveedor", "obra", SUM("total") AS total_cemento_proveedor FROM {TABLE_NAME} WHERE "subcategoria" = 'CEMENTO' AND "obra" IN ('Bodega Acatlán E2', 'K. Las Vias') GROUP BY "proveedor", "obra" ORDER BY "proveedor", total_cemento_proveedor DESC;`
        
        """),
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{user_input}"),
    ])

    chain = prompt_template | llm | StrOutputParser()
    
    sql_query = chain.invoke({
        "table_schema": state["table_schema"],
        "user_input": state["user_input"],
        "chat_history": state["chat_history"],
        "streamlit_filters": state["streamlit_filters"],
        "corrected_entities": state.get("corrected_entities", {}),
        "sql_error": state.get("sql_error", "None"),
        "subcategoria": subcategoria
    })
    
    # Basic cleaning of the SQL query
    if '```sql' in sql_query:
        sql_query = sql_query.split('```sql')[1].split('```')[0].strip()
    elif '```' in sql_query:
        sql_query = sql_query.split('```')[1].strip()
        
    print(f"Generated SQL Query: {sql_query}")
    return {"sql_query": sql_query, "sql_error": None} # Reset error before execution

def execute_sql_node(state: AgentState):
    print("--- Running: execute_sql_node ---")
    sql_query = state.get("sql_query")
    if not sql_query:
        print("No SQL query to execute.")
        return {"sql_error": "No SQL query was generated.", "results_df": None}

    db = get_sql_database_tool()
    try:
        # Using pandas to directly get a DataFrame
        # db.run(sql_query) returns a string. For DataFrame, we need to query directly.
        with get_db_engine().connect() as connection:
            results_df = pd.read_sql_query(sql_query, connection)
        
        # Convert any UUID columns to strings to avoid PyArrow conversion errors
        for col in results_df.columns:
            # Check if column contains UUID objects
            if results_df[col].dtype == 'object':
                # Sample the first non-null value to check its type
                sample = results_df[col].dropna().iloc[0] if not results_df[col].dropna().empty else None
                if sample is not None and isinstance(sample, uuid.UUID):
                    print(f"Converting UUID column {col} to string")
                    results_df[col] = results_df[col].astype(str)
                elif 'uuid' in col.lower() and results_df[col].dtype == 'object':
                    # If column name contains 'uuid', assume it might be UUID and convert to be safe
                    print(f"Column name contains 'uuid': {col}, converting to string")
                    results_df[col] = results_df[col].apply(lambda x: str(x) if x is not None else None)
        
        print(f"SQL Execution successful. Results shape: {results_df.shape}")
        return {"results_df": results_df, "sql_error": None}
    except Exception as e:
        error_message = f"SQL Execution Error: {str(e)}"
        print(error_message)
        return {"results_df": None, "sql_error": error_message}

def generate_response_node(state: AgentState):
    print("--- Running: generate_response_node ---")
    if state.get("clarification_question"):
        print(f"Responding with clarification: {state['clarification_question']}")
        return {
            "natural_response": state["clarification_question"],
            "chat_history": state["chat_history"] + [AIMessage(content=state["clarification_question"])]
        }

    results_df = state.get("results_df")
    user_input = state["user_input"]
    
    if results_df is None or results_df.empty:
        response_text = "No results found for your query."
        if state.get("sql_error"):
            response_text = f"I encountered an issue with the query. {state['sql_error']}"
    else:
        # Convert DataFrame to a JSON string format that the LLM can process
        # Limit to first 50 rows to avoid token limits
        data_sample = results_df.head(50)
        data_json = data_sample.to_json(orient='records', date_format='iso')
        
        # Create prompt for GPT-4.1
        prompt = ChatPromptTemplate.from_messages([
            ("system", """
            Basado en la siguiente pregunta del usuario y los datos SQL resultantes, genera un resumen analítico y profesional en español. Tu resumen debe:
            1. Responder directamente a la pregunta del usuario.
            2. Describir los hallazgos clave de los datos, como el número de registros, valores importantes, sumas, promedios, máximos o mínimos si son relevantes y se pueden inferir directamente de los datos proporcionados.
            3. Identificar patrones o puntos destacables sin hacer suposiciones sobre columnas específicas. Analiza los datos tal como están.
            4. Mantener un tono profesional y orientado a la toma de decisiones.
            5. Ser conciso pero informativo.
            6. Asegúrate de que si hay montos, se mencione la moneda si está presente en los datos.
            
            Responde únicamente con el resumen analítico, sin incluir introducciones o mensajes adicionales.
            """),
            ("human", """Pregunta del Usuario: {user_input}
            
            Datos SQL (muestra de hasta 50 filas):
            {data_json}
            
            Total de filas en el conjunto completo de datos: {total_rows}
            """)
        ])
        
        # Generate response using GPT-4.1
        try:
            chain = prompt | llm | StrOutputParser()
            response_text = chain.invoke({
                "user_input": user_input,
                "data_json": data_json,
                "total_rows": len(results_df)
            })
            print(f"Generated analytical response with GPT-4.1")
        except Exception as e:
            print(f"Error generating LLM response: {str(e)}")
            # Fallback to simple response
            response_text = f"He encontrado {len(results_df)} resultados para tu consulta: '{user_input}'."

    print(f"Natural response: {response_text}")
    return {
        "natural_response": response_text,
        "chat_history": state["chat_history"] + [AIMessage(content=response_text)]
    }

def handle_error_or_clarify_node(state: AgentState):
    print("--- Running: handle_error_or_clarify_node ---")
    sql_error = state.get("sql_error")
    # Simple strategy: always try to regenerate SQL once on error.
    # A more sophisticated approach might involve asking for clarification or specific LLM for error correction.
    if sql_error: # If there was an error during SQL execution
        # For now, we'll just pass the error to the generate_sql_query_node for a retry.
        # A more advanced system could ask for clarification here.
        print(f"SQL error detected: {sql_error}. Will attempt to regenerate SQL.")
        return {"sql_error": sql_error, "clarification_question": None} # Signal to retry SQL generation
    else:
        # This case should ideally not be reached if routing is correct, but as a fallback:
        print("No SQL error, but in handle_error_or_clarify. Asking for clarification.")
        clarification = "I'm having a bit of trouble understanding. Could you please rephrase or provide more details?"
        return {"clarification_question": clarification, "sql_error": None}

# --- New Nodes for Semantic Query Support ---

def determine_query_type_node(state: AgentState):
    print("--- Running: determine_query_type_node ---")
    
    user_input = state["user_input"]
    corrected_entities = state["corrected_entities"]
    subcategoria = state["subcategorias_disponibles"]
    
    # Determine if we need semantic search or static SQL
    query_type_prompt = ChatPromptTemplate.from_messages([
        ("system", """
        Eres un clasificador inteligente de consultas. Tu tarea es analizar las preguntas de los usuarios sobre facturas y determinar si requieren una **BÚSQUEDA SEMÁNTICA** (para encontrar elementos relevantes basados en significado y descripción) o una **CONSULTA SQL ESTÁTICA** (para realizar cálculos y agregaciones).

        **Clasifica como BÚSQUEDA SEMÁNTICA cuando:**

        * **La pregunta central es sobre *existencia* o *descubrimiento*, Y NO pide una cantidad de una 'subcategoria' conocida.** El usuario pregunta *si* algo existe, o pide *encontrar* facturas basándose en su naturaleza o contenido (ej. "¿Existen facturas de...?", "¿Hay registros sobre...?", "¿Encuentra facturas que mencionen...?").
        * **La búsqueda se basa en *conceptos, descripciones o sinónimos*, especialmente cuando no se menciona una 'subcategoria' específica o el término es amplio/difuso.** El usuario emplea términos que requieren comprender el significado, el contexto o la similitud (ej. "seguridad industrial", "consultoría ambiental", "pasajuntas o similares").
        * **La respuesta *no* se puede obtener mediante agregaciones SQL estándar (SUM, COUNT, AVG, MAX, MIN).** El objetivo no es un número, sino identificar *qué* facturas (si las hay) coinciden con una descripción.
        * **Implica una búsqueda dentro de *campos textuales* (como 'descripcion' o 'concepto').** La consulta sugiere buscar *dentro* de los detalles de una factura para encontrar significado.
        * **Se implica la *coincidencia aproximada* (fuzzy matching) o la *relevancia*.** El usuario busca elementos que son *parecidos* a algo o *relacionados* con un tema.

        **Clasifica como CONSULTA SQL ESTÁTICA cuando:**

        * **Regla Crucial: Si la pregunta pide *cuantificación* (Cuánto, Cuántos, Total, Suma, Conteo) y menciona un material/servicio específico que *coincide o es muy probable* que sea una 'subcategoria' ejemplo: {subcategoria}, **DEBE ser ESTÁTICA**. La intención es realizar un cálculo sobre una categoría conocida, incluso si la pregunta usa términos como "comprado" o "usado".
        * **La pregunta central pide *cuantificación*.** El usuario pregunta explícitamente "¿Cuánto?", "¿Cuántos?", "¿Cuál es el total/promedio/máximo/mínimo?" (ej. "¿Cuánto ha sido el total...?", "¿Cuál es el gasto promedio...?").
        * **Requiere *cálculos o agregaciones específicas*.** La consulta se asigna directamente a funciones SQL como `SUM`, `COUNT`, `AVG`, `MAX`, `MIN`.
        * **Involucra *agrupación u ordenamiento* basado en cálculos.** El usuario pide resultados "por obra", "por proveedor", o quiere el "mayor" o "menor".
        * **Se basa en *coincidencias exactas* con campos conocidos y estructurados.** La consulta utiliza valores específicos para 'obra', 'residente', 'proveedor', 'categoria_id', 'subcategoria', fechas, etc.
        * **La respuesta es un *número, lista o conteo específico* derivado directamente de datos estructurados.**

        Tu resultado debe ser únicamente 'SEMANTIC' o 'STATIC'.

        Devuelve SÓLO uno de los dos valores:
        - "SEMANTIC" si la consulta requiere búsqueda semántica
        - "STATIC" si la consulta puede ser manejada con una consulta SQL estática

        **Ejemplos:**

        1.  "¿Cuánto ha sido el **total gastado** en la obra 'K. Las Vias'?" → `STATIC` (Es una agregación de suma - `SUM`).
        2.  "¿Se han pagado facturas de **pasajuntas o similares de herrería** en la obra 'Bodega Acatlán E2'?" → `SEMANTIC` (Busca elementos similares o conceptuales, requiere entender 'pasajuntas' y 'herrería').
        3.  "Muestra las **5 facturas con mayor importe** del proveedor 'Home Depot' para la obra 'Casa CA'." → `STATIC` (Implica ordenar y limitar - `ORDER BY`, `LIMIT`).
        4.  "¿Hay facturas de materiales **parecidos a la varilla de construcción** en la obra 'Casa ZDT'?" → `SEMANTIC` (Busca materiales similares por descripción, requiere entender 'varilla' y sus equivalentes).
        5.  "¿Cuál es el **costo promedio** de las facturas de la categoría 'ACERO' en la obra 'Muro Altozano'?" → `STATIC` (Es un cálculo de promedio - `AVG`).
        6.  "¿Tenemos alguna factura relacionada con **servicios de impermeabilización o sellado de techos** para el 'Capilla Altozano'?" → `SEMANTIC` (Busca por conceptos y términos relacionados, no una categoría exacta).
        7.  "¿**Cuántas facturas** tiene pendientes de pago el residente 'Ing. Noe' en la obra 'Estacionamiento Coca Cola'?" → `STATIC` (Es una agregación de conteo - `COUNT`).
        8.  "Busca facturas que mencionen **retroexcavadoras o máquinas 'mano de chango'** para la obra 'Bosques de la Cantera'." → `SEMANTIC` (Busca ítems específicos con posibles sinónimos o nombres coloquiales).
        9.  "Desglosa el **gasto total por categoría** para la obra 'Hospital General Regional'." → `STATIC` (Implica suma y agrupación - `SUM`, `GROUP BY`).
        10. "¿Existen facturas por **estudios de impacto ambiental** o **manifestaciones ecológicas** en el proyecto 'K. Niños Heroes Tecoman'?" → `SEMANTIC` (Busca tipos de servicio complejos y descriptivos que probablemente no sean una categoría fija).
        11. "¿**Cuánto cemento** se ha comprado en la obra 'Bodega Acatlán E2'?" → `STATIC` (Pide cuantificación -Cuánto- de una subcategoría conocida -Cemento-).
        """),
        ("human", """
        Pregunta del usuario: {user_input}
        
        Entidades extraídas: {corrected_entities}

        Considera la lista de subcategorías disponibles: {subcategoria}
        
        Determina si esta consulta requiere búsqueda semántica o una consulta SQL estática:
        """)
    ])
    
    query_type_chain = query_type_prompt | llm | StrOutputParser()
    
    try:
        query_type_response = query_type_chain.invoke({
            "user_input": user_input,
            "corrected_entities": corrected_entities,
            "subcategoria": subcategoria
        }).strip().upper()
        
        print(f"Query type determined: {query_type_response}")
        
        # Normalize response to ensure we get one of our expected values
        if "SEMANTIC" in query_type_response:
            query_type = "SEMANTIC"
        else:
            # Default to static if response is unclear
            query_type = "STATIC"
    except Exception as e:
        print(f"Error determining query type: {e}")
        # Default to static SQL on error
        query_type = "STATIC"
    
    return {
        **state,  # Keep all existing state
        "query_type": query_type  # Add query_type to state
    }

def generate_semantic_sql_query_node(state: AgentState):
    print("--- Running: generate_semantic_sql_query_node ---")
    TABLE_NAME = "portal_desglosado"
    
    # Get inputs from state
    user_input = state["user_input"]
    corrected_entities = state["corrected_entities"]
    table_schema = state["table_schema"]
    
    # Get the description for semantic search
    description_text = corrected_entities.get("descripcion", "")
    
    if not description_text:
        print("Warning: No description found for semantic search. Using user input as fallback.")
        description_text = user_input
    
    # Generate embedding for the description
    try:
        query_embedding = embeddings_model.embed_query(description_text)
        query_embedding_str = f"[{','.join(map(str, query_embedding))}]"
        
        # Start building the semantic SQL query
        sql_query = f"SELECT * FROM {TABLE_NAME}"
        
        # Add WHERE clauses for any entities we have (like obra or proveedor)
        where_clauses = []
        for entity_key, entity_value in corrected_entities.items():
            # Skip the description itself since we're using it for vector search
            if entity_key != "descripcion" and entity_value and isinstance(entity_value, str):
                where_clauses.append(f"\"{entity_key}\" = '{entity_value}'")
        
        if where_clauses:
            sql_query += " WHERE " + " AND ".join(where_clauses)
        
        # Add the vector similarity ordering
        sql_query += f" ORDER BY embedding <-> '{query_embedding_str}'::vector LIMIT 10;"
        
        return {
            **state,  # Keep all existing state
            "sql_query": sql_query,
            "sql_error": None  # Reset error before execution
        }
    except Exception as e:
        error_message = f"Error generating semantic SQL: {str(e)}"
        print(error_message)
        return {
            **state,  # Keep all existing state
            "sql_error": error_message
        }

# --- Graph Definition ---

workflow = StateGraph(AgentState)

# Add nodes
workflow.add_node("process_input", process_input_and_extract_entities_node)
workflow.add_node("determine_query_type", determine_query_type_node)
workflow.add_node("generate_sql", generate_sql_query_node)
workflow.add_node("generate_semantic_sql", generate_semantic_sql_query_node)
workflow.add_node("execute_sql", execute_sql_node)
workflow.add_node("generate_response", generate_response_node)
workflow.add_node("handle_error", handle_error_or_clarify_node)

# Define edges
workflow.set_entry_point("process_input")
workflow.add_edge("process_input", "determine_query_type")

# Conditional edge from determine_query_type to appropriate SQL generation method
def route_to_sql_generator(state: AgentState):
    query_type = state.get("query_type", "STATIC")  # Default to static if not specified
    print(f"Routing query to {query_type} SQL generator")
    if query_type == "SEMANTIC":
        return "semantic"
    else:
        return "static"

workflow.add_conditional_edges(
    "determine_query_type",
    route_to_sql_generator,
    {
        "semantic": "generate_semantic_sql",
        "static": "generate_sql"
    }
)

# Connect both SQL generation paths to execute_sql
workflow.add_edge("generate_sql", "execute_sql")
workflow.add_edge("generate_semantic_sql", "execute_sql")

# Conditional Edges from execute_sql
def decide_after_sql_execution(state: AgentState):
    if state.get("sql_error"):
        return "handle_error"
    return "generate_response"

workflow.add_conditional_edges(
    "execute_sql",
    decide_after_sql_execution,
    {
        "handle_error": "handle_error",
        "generate_response": "generate_response"
    }
)

# Conditional Edges from handle_error
def decide_after_error_handling(state: AgentState):
    # If we formulated a clarification question, go to generate_response to output it.
    # Otherwise, if sql_error is still present (meaning we want to retry SQL), go to generate_sql.
    if state.get("clarification_question"):
        return "ask_clarification"
    # Simple retry logic: if an error occurred, try to regenerate SQL once.
    # This assumes handle_error_node sets sql_error if a retry is desired.
    # A counter for retries might be good in AgentState for more complex logic.
    if state.get("sql_error"):
         # Check if we've already tried once (not implemented here, but good for future)
        return "retry_sql_generation"
    return "end_process_unexpectedly" # Should not happen with current logic

workflow.add_conditional_edges(
    "handle_error",
    decide_after_error_handling,
    {
        "ask_clarification": "generate_response", # To output the clarification question
        "retry_sql_generation": "generate_sql",
        "end_process_unexpectedly": END # Fallback, ideally not reached
    }
)

workflow.add_edge("generate_response", END)

# Compile the graph
# memory = SqliteSaver.from_conn_string(":memory:") # For persistent memory if needed
# app = workflow.compile(checkpointer=memory)
app = workflow.compile()

# --- Main function to be called by Streamlit ---
def run_sql_agent(user_input: str, chat_history: List[BaseMessage], streamlit_filters: Dict[str, List[str]], obras_disponibles: List[str], proveedores_disponibles: List[str], subcategorias_disponibles: List[str], session_id: str = None, user_id: str = None):
    # Generar IDs si no se proporcionan
    if not session_id:
        session_id = str(uuid.uuid4())
    
    start_time = time.time()
    
    initial_state: AgentState = {
        "user_input": user_input,
        "chat_history": chat_history,
        "table_schema": "", # Will be populated by a node
        "extracted_entities": {},
        "corrected_entities": {},
        "streamlit_filters": streamlit_filters,
        "query_type": None,
        "sql_query": None,
        "results_df": None,
        "natural_response": None,
        "sql_error": None,
        "clarification_question": None,
        "obras_disponibles": obras_disponibles,          # New
        "proveedores_disponibles": proveedores_disponibles, # New
        "subcategorias_disponibles": subcategorias_disponibles # New
    }
    
    # Ensure OPENAI_API_KEY is available
    if not os.getenv("OPENAI_API_KEY") and not (st.secrets.get("openai") and st.secrets["openai"].get("OPENAI_API_KEY")):
        st.error("OpenAI API key not found. Please set it in st.secrets or as an environment variable.")
        return {"natural_response": "Error: OpenAI API key not configured.", "results_df": None, "chat_history": chat_history}
    if os.getenv("OPENAI_API_KEY") is None and st.secrets.get("openai") and st.secrets["openai"].get("OPENAI_API_KEY"):
        os.environ["OPENAI_API_KEY"] = st.secrets["openai"]["OPENAI_API_KEY"]

    final_state = app.invoke(initial_state)
    
    # Calcular tiempo de respuesta
    end_time = time.time()
    response_time_ms = int((end_time - start_time) * 1000)
    
    # Registrar la interacción
    log_chat_interaction(
        session_id=session_id,
        user_id=user_id or "anonymous",
        user_input=user_input,
        corrected_entities=final_state.get("corrected_entities", {}),
        generated_sql=final_state.get("sql_query", ""),
        query_type=final_state.get("query_type", "STATIC"),
        sql_error=final_state.get("sql_error", ""),
        execution_success=final_state.get("sql_error") is None,
        response_time_ms=response_time_ms
    )
    
    return {
        "natural_response": final_state.get("natural_response", "No response generated."),
        "results_df": final_state.get("results_df"),
        "chat_history": final_state.get("chat_history", chat_history),
        "session_id": session_id  # Devolver el ID para mantener consistencia
    }

if __name__ == '__main__':
    # This is for local testing of the agent, not for Streamlit deployment
    print("Testing SQL Agent locally...")

