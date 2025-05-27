# Configuration file for StreamlPT application

# Supabase configuration
SUPABASE_CONFIG = {
    # Default table name for the application
    "KIOSKO_VISTA": "vista_facturas_kiosko",
    "DESGLOSADO": "portal_desglosado",
    "CONTABILIDAD": "portal_contabilidad",
    "CONCENTRADO": "portal_concentrado",
    
    # Default columns to retrieve from the table
    "DEFAULT_COLUMNS": [
        "obra", "folio", "fecha_factura", "cantidad", "subtotal", "total", 
        "descripcion", "categoria_id", "subcategoria", "cuenta_gasto", "tipo_gasto", 
        "proveedor", "residente", "estatus", "fecha_recepcion", "fecha_pagada", 
        "fecha_autorizacion", "clave_producto", "clave_unidad", 
        "unidad", "precio_unitario", "descuento", "venta_tasa_0", "venta_tasa_16", 
        "moneda", "total_iva", "total_ish", "retencion_iva", "retencion_isr", 
        "serie", "url_pdf", "url_oc", "url_rem", "xml_uuid"
    ],
    "KIOSKO_VISTA_COLUMNS": [
        "uuid_concepto", "cuenta_gasto", "obra", "tipo_gasto", "proveedor", 
        "residente", "folio", "estatus", "fecha_factura", "fecha_recepcion", 
        "fecha_pagada", "fecha_autorizacion", "clave_producto", "clave_unidad", 
        "cantidad", "descripcion", "categoria_id", "subcategoria", 
        "encontrado_en_diccionario", "confianza_prediccion", "unidad", 
        "precio_unitario", "subtotal", "descuento", "venta_tasa_0", 
        "venta_tasa_16", "moneda", "total_iva", "total_ish", "retencion_iva", 
        "retencion_isr", "total", "serie", "url_pdf", "url_oc", "url_rem", 
        "xml_uuid", "sat"
    ],
    "CONSULTA": [
        "fecha_consulta"
    ],
    
    # Columns used for filtering data
    "FILTER_COLUMNS": [
        "obra", "proveedor", "categoria_id", "subcategoria", 
        "residente", "estatus", "moneda", "unidad"
    ],
    
    # Column mapping for display (database column name -> display name)
    "COLUMN_MAPPING": {
        "obra": "Obra",
        "cuenta_gasto": "Cuenta Gasto",
        "folio": "Folio",
        "tipo_gasto": "Tipo Gasto",
        "descripcion": "Descripción",
        "fecha_factura": "Fecha Factura",
        "cantidad": "Cantidad",
        "total": "Total",
        "subtotal": "Subtotal",
        "subcategoria": "Subcategoría",
        "categoria_id": "Categoría",
        "url_pdf": "Factura",
        "url_oc": "Orden de Compra",
        "url_rem": "Remisión",
        "proveedor": "Proveedor",
        "residente": "Residente",
        "estatus": "Estatus",
        "fecha_recepcion": "Fecha Recepción",
        "fecha_pagada": "Fecha Pagado",
        "fecha_autorizacion": "Fecha Autorización",
        "clave_producto": "Clave Producto",
        "clave_unidad": "Clave Unidad",
        "unidad": "Unidad",
        "precio_unitario": "Precio Unitario",
        "descuento": "Descuento",
        "venta_tasa_0": "Venta Tasa 0%",
        "venta_tasa_16": "Venta Tasa 16%",
        "moneda": "Moneda",
        "total_iva": "IVA 16%",
        "total_ish": "ISH",
        "retencion_iva": "Retención IVA",
        "retencion_isr": "Retención ISR",
        "serie": "Serie",
        "xml_uuid": "UUID",
    }
}

# Function to get configuration values
def get_config(key=None):
    """
    Get configuration values from the SUPABASE_CONFIG dictionary
    
    Args:
        key: Optional key to retrieve a specific configuration value
             If None, returns the entire configuration dictionary
    
    Returns:
        The requested configuration value or the entire configuration dictionary
    """
    if key is None:
        return SUPABASE_CONFIG
    
    return SUPABASE_CONFIG.get(key)
