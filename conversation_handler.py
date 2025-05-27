import json
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import streamlit as st

# Initialize LLM (same as in sql_agent.py)
openai_api_key = st.secrets["openai"]["OPENAI_API_KEY"]
conversation_llm = ChatOpenAI(model="gpt-4.1-mini-2025-04-14", temperature=0.2, api_key=openai_api_key)

def determine_conversation_intent(user_input: str, chat_history: List[BaseMessage]) -> Dict[str, Any]:
    """
    Determine if the user's input requires SQL execution or can be handled conversationally.
    
    Args:
        user_input: The user's input text
        chat_history: Previous conversation messages
        
    Returns:
        Dictionary with classification results including:
        - 'needs_sql': Boolean indicating if SQL execution is needed
        - 'intent_type': String describing the intent (e.g., 'sql_query', 'clarification', 'small_talk')
        - 'confidence': Float between 0-1 indicating confidence in classification
        - 'reasoning': String explaining the classification reasoning
    """
    # Check for explicit history reference prefix
    if user_input.strip().startswith("@H"):
        # Remove the prefix for further processing
        user_input = user_input.strip()[2:].strip()
        return {
            "needs_sql": False,
            "intent_type": "follow_up",
            "confidence": 1.0,
            "reasoning": "Usuario utilizó el prefijo @H para indicar referencia explícita al historial"
        }
        
    # Format chat history for context
    formatted_history = ""
    for msg in chat_history[-10:]:  # Use last 10 messages for context
        role = "Usuario" if isinstance(msg, HumanMessage) else "Asistente"
        formatted_history += f"{role}: {msg.content}\n"
    
    # Create the prompt for intent classification
    intent_prompt = ChatPromptTemplate.from_messages([
        ("system", """
        Eres un asistente especializado en consultas de bases de datos de facturación para proyectos de construcción.
        
        Debes responder de manera conversacional, amigable y profesional a la entrada del usuario sin ejecutar SQL.
        La intención del usuario ha sido clasificada como: {intent_type} con razonamiento: {reasoning}
        
        Guías para tu respuesta:
        1. Si es small_talk: Responde de manera amable y breve, manteniendo el enfoque profesional
        2. Si es clarification: Proporciona más detalles sobre tu respuesta anterior si es posible
        3. Si es general_question: Explica conceptos generales sobre la base de datos o el sistema
        4. Si es follow_up: 
           - ANALIZA CUIDADOSAMENTE las tablas de datos ya mostradas en respuestas anteriores
           - BUSCA ATENTAMENTE todos los valores numéricos relevantes en las tablas de resultados previos
           - REALIZA CÁLCULOS PRECISOS (sumas, promedios, conteos) usando los valores de las tablas mostradas
           - Si se pide información sobre un proveedor específico, IDENTIFICA todas las filas asociadas a ese proveedor
           - MUESTRA DETALLADAMENTE cómo realizaste el cálculo para transparencia
           - NO pidas información adicional si los datos ya están disponibles en las tablas previas
        
        Contexto de la conversación previa:
        {chat_history}
        
        Tu respuesta debe ser:
        - Informativa y útil
        - Profesional con tono amigable
        - Concisa sin ser demasiado breve
        - Contextualmente relevante a la conversación previa
        - EXACTA y PRECISA cuando se refiera a datos numéricos ya mostrados
        
        INSTRUCCIONES ESPECIALES PARA CONSULTAS CON "@H":
        1. SIEMPRE busca en todo el historial de chat las tablas de datos mostradas
        2. Cuando encuentres una tabla, EXTRAE los valores numéricos relevantes
        3. Si se pregunta por un proveedor específico, FILTRA las filas por ese proveedor
        4. CALCULA las sumas o totales solicitados basándote SOLO en esos datos
        5. NUNCA digas que necesitas ejecutar SQL si los datos ya están visibles en las tablas previas
        
        Si realmente la información solicitada NO aparece en ninguna tabla previa, entonces puedes indicar que se necesitaría una consulta adicional.
        """),
        ("human", "{user_input}")
    ])
    
    # Chain for intent classification
    intent_chain = intent_prompt | conversation_llm | StrOutputParser()
    
    try:
        # Get classification result
        result = intent_chain.invoke({"chat_history": formatted_history, "user_input": user_input})
        intent_data = json.loads(result)
        
        # Ensure all required fields are present
        if not all(k in intent_data for k in ["needs_sql", "intent_type", "confidence", "reasoning"]):
            # Add missing fields with defaults
            intent_data = {
                "needs_sql": intent_data.get("needs_sql", True),  # Default to SQL for safety
                "intent_type": intent_data.get("intent_type", "sql_query"),
                "confidence": intent_data.get("confidence", 0.5),
                "reasoning": intent_data.get("reasoning", "Clasificación incompleta")
            }
            
        return intent_data
    except Exception as e:
        print(f"Error during intent classification: {e}")
        # Default to SQL path on error for safety
        return {
            "needs_sql": True,
            "intent_type": "sql_query",
            "confidence": 0.0,
            "reasoning": f"Error en clasificación: {str(e)}"
        }

def generate_conversational_response(
    user_input: str,
    chat_history: list,
    intent_data: dict,
    sql_results_json: str = None,
):
    """
    Generate a conversational response without executing SQL.
    
    Args:
        user_input: The user's input text
        chat_history: Previous conversation messages
        intent_data: Classification data from determine_conversation_intent
        sql_results_json: JSON string containing previous SQL results
        
    Returns:
        Dictionary with response data:
        - 'natural_response': String containing the conversational response
        - 'results_df': None (no SQL results for conversational responses)
    """
    # Format chat history for context
    formatted_history = ""
    
    # For follow-up questions, use all available history to ensure we have the data tables
    if intent_data.get("intent_type") == "follow_up":
        messages_to_include = chat_history  # Use all messages for follow-up questions
        print("\n[DEBUG] Using FULL history for follow-up question. Total messages:", len(chat_history))
    else:
        messages_to_include = chat_history[-5:]  # Use last 5 messages for other intents
        print("\n[DEBUG] Using last 5 messages for regular question. Total messages:", len(messages_to_include))

    for msg in messages_to_include:
        role = "Usuario" if isinstance(msg, HumanMessage) else "Asistente"
        formatted_history += f"{role}: {msg.content}\n"
        
        # Print the full formatted history for debugging
        print("\n[DEBUG] Formatted history being sent to model:\n", "-"*80)
        print(formatted_history)
        print("-"*80)
    
    # Create the prompt for conversational response
    conversation_prompt = ChatPromptTemplate.from_messages([
        ("system", """
        Eres un asistente especializado en consultas de bases de datos de facturación para proyectos de construcción.
        
        Debes responder de manera conversacional, amigable y profesional a la entrada del usuario sin ejecutar SQL.
        La intención del usuario ha sido clasificada como: {intent_type} con razonamiento: {reasoning}
        
        Guías para tu respuesta:
        1. Si es small_talk: Responde de manera amable y breve, manteniendo el enfoque profesional
        2. Si es clarification: Proporciona más detalles sobre tu respuesta anterior si es posible
        3. Si es general_question: Explica conceptos generales sobre la base de datos o el sistema
        4. Si es follow_up: Refiérete a la información ya mostrada, haz aclaraciones sobre datos previos
        
        Contexto de la conversación previa:
        {chat_history}

        Datos SQL (muestra de hasta 50 filas):
        {sql_results_json}
        
        Tu respuesta debe ser:
        - Informativa y útil
        - Profesional con tono amigable
        - Concisa sin ser demasiado breve
        - Contextualmente relevante a la conversación previa
        
        NO INVENTES DATOS que requerirían una consulta SQL. Si no puedes responder sin acceder a la base de datos,
        sugiere al usuario reformular su pregunta para obtener datos específicos.
        """),
        ("human", "{user_input}")
    ])
    
    # Chain for conversation generation
    conversation_chain = conversation_prompt | conversation_llm | StrOutputParser()
    
    try:
        # Generate conversational response
        response = conversation_chain.invoke({
            "chat_history": formatted_history, 
            "user_input": user_input,
            "intent_type": intent_data.get("intent_type", "general_question"),
            "reasoning": intent_data.get("reasoning", ""),
            "sql_results_json": sql_results_json
        })
        
        return {
            "natural_response": response,
            "results_df": None  # No SQL results for conversational responses
        }
    except Exception as e:
        print(f"Error generating conversational response: {e}")
        # Return a generic error response
        return {
            "natural_response": "Lo siento, tuve un problema al procesar tu mensaje. ¿Podrías reformularlo de otra manera?",
            "results_df": None
        }
