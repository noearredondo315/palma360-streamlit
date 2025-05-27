import streamlit as st

@st.dialog("Estado de Carga de Datos", width="large")
def loading_data_dialog():
    """
    Diálogo modal para mostrar el progreso de carga de datos.
    Este diálogo es puramente para visualización y se controla mediante st.session_state.
    
    Requiere las siguientes claves en st.session_state:
    - dialog_progress_value (float): 0.0 a 1.0, para la barra de progreso.
    - dialog_progress_message (str): Mensaje general de estado.
    - dialog_detailed_messages (list): Lista de dicts 
      {'type': 'info/success/warning/error', 'table': str, 'message': str}
    - dialog_loading_finished (bool): True si la carga ha terminado.
    - dialog_overall_success (bool): True si la carga general fue exitosa.
    """
    st.markdown("### Estado de la Carga de Datos del Sistema")

    progress_value = st.session_state.get('dialog_progress_value', 0.0)
    progress_message = st.session_state.get('dialog_progress_message', "Iniciando...")
    detailed_messages = st.session_state.get('dialog_detailed_messages', [])
    loading_finished = st.session_state.get('dialog_loading_finished', False)
    overall_success = st.session_state.get('dialog_overall_success', True) # Assume success unless told otherwise

    st.progress(float(progress_value), text=str(progress_message))
    
    status_text_ui = st.empty()
    status_text_ui.info(str(progress_message)) # Display the latest general status message

    if detailed_messages:
        st.markdown("---_Detalles de la Carga:_---")
        # Display messages in reverse for latest first, or normal for chronological
        for msg_info in reversed(detailed_messages):
            table = msg_info.get('table', 'General')
            message = msg_info.get('message', '')
            msg_type = msg_info.get('type', 'info')

            formatted_message = f"**[{table}]**: {message}"
            if msg_type == "success":
                st.success(formatted_message)
            elif msg_type == "warning":
                st.warning(formatted_message)
            elif msg_type == "error":
                st.error(formatted_message)
            else: # 'info' or other
                st.info(formatted_message)

    if loading_finished:
        final_message = "Carga de datos completada."
        if not overall_success:
            final_message = "Carga de datos completada con errores."
        elif any(m.get('type') == 'warning' for m in detailed_messages):
            final_message = "Carga de datos completada con advertencias."
        
        status_text_ui.info(final_message) # Update with final overall status
        
        st.markdown("---_Acciones:_---")
        if st.button("Cerrar"):
            st.session_state.dialog_is_open = False # Signal to the caller to stop showing the dialog
            # Clean up dialog-specific session state variables
            keys_to_delete = [
                'dialog_progress_value', 'dialog_progress_message',
                'dialog_detailed_messages', 'dialog_loading_finished',
                'dialog_overall_success', 'dialog_loader_thread_active'
            ]
            for key in keys_to_delete:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun() # Rerun the main page to reflect dialog closure and state cleanup
    else:
        # If not finished, the calling page should be handling st.rerun() to keep this dialog updated.
        # This dialog itself doesn't loop with st.rerun().
        pass
