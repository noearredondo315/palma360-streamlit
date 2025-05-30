import streamlit as st
from streamlit_lottie import st_lottie

@st.dialog("Estado de Carga de Datos", width="large")
def loading_data_dialog():
    """
    Diálogo modal para mostrar el progreso de carga de datos.
    Este diálogo es puramente para visualización y se controla mediante st.session_state.
    
    Requiere las siguientes claves en st.session_state:
    - dialog_progress_value (float): 0.0 a 1.0, para la barra de progreso.
    - dialog_progress_message (str): Mensaje general de estado.
    - dialog_loading_finished (bool): True si la carga ha terminado.
    """
    # Lottie animation
    st_lottie(
        "https://lottie.host/dbf50f2e-c792-4f7d-a76a-0754cb5a4473/8SpiIA23TV.json",
        speed=0.8,
        height=400,
        loop=True,
        reverse=False,
        quality="high",
    )
    
    # Progress bar
    progress_value = st.session_state.get('dialog_progress_value', 0.0)
    progress_message = st.session_state.get('dialog_progress_message', "Iniciando...")
    loading_finished = st.session_state.get('dialog_loading_finished', False)
    
    st.progress(float(progress_value), text=str(progress_message))
    
    # Check if loading is finished to show close button
    if loading_finished:
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
