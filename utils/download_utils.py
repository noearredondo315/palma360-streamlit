"""
Utilidades para descarga de archivos desde URLs en múltiples hilos.
"""
import os
import time
import requests
import threading
import queue
import io
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
import mimetypes
import logging
import uuid

# Importar PyPDF2 para combinar PDFs
try:
    from PyPDF2 import PdfMerger, PdfReader
    HAS_PYPDF2 = True
except ImportError:
    HAS_PYPDF2 = False
    logging.warning("PyPDF2 no está instalado. La funcionalidad de combinación de PDFs no estará disponible.")
    logging.warning("Para instalarlo, ejecuta: pip install PyPDF2")

# Importar Pillow para conversión de imágenes a PDF
try:
    from PIL import Image
    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False
    logging.warning("Pillow no está instalado. La conversión de imágenes a PDF no estará disponible.")
    logging.warning("Para instalarlo, ejecuta: pip install Pillow")

# Formatos de archivo que son imágenes
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp']

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DescargadorArchivos")

class DescargadorArchivo:
    """
    Clase para descargar archivos desde URLs con reintentos automáticos
    y conversión automática de imágenes a PDF cuando se requiere
    """
    def __init__(self, url, ruta_destino, convertir_a_pdf=True):
        self.url = url
        self.ruta_destino = ruta_destino
        self.content_type = None
        self.convertir_a_pdf = convertir_a_pdf

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30))
    def descargar_archivo(self):
        """Descarga un archivo desde una URL con reintentos"""
        try:
            response = requests.get(self.url, timeout=30, stream=True)
            response.raise_for_status()
            self.content_type = response.headers.get('Content-Type', '')
            return response.content
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al descargar {self.url}: {e}")
            raise

    def es_imagen(self, content=None, content_type=None):
        """Determina si el contenido o el Content-Type corresponde a una imagen
        
        Intenta detectar si el contenido es una imagen de dos formas:
        1. Comprobando si el Content-Type indica que es una imagen
        2. Intentando abrir el contenido con PIL para confirmar
        """
        # Verificar por tipo MIME
        if content_type:
            if content_type.startswith('image/'):
                return True
                
            # Mapeo adicional de tipos MIME
            mime_map_images = [
                'image/jpeg', 'image/png', 'image/gif', 'image/bmp', 
                'image/tiff', 'image/webp'
            ]
            if any(mime in content_type for mime in mime_map_images):
                return True
                
        # Verificar intentando abrir con PIL si tenemos el contenido
        if content and HAS_PILLOW:
            try:
                with Image.open(io.BytesIO(content)) as img:
                    return True
            except Exception:
                pass
                
        return False

    def convertir_imagen_a_pdf(self, contenido=None, ruta_imagen=None):
        """Convierte una imagen a PDF desde contenido en bytes o desde archivo
        
        Args:
            contenido: Bytes de la imagen (opcional)
            ruta_imagen: Ruta del archivo de imagen (opcional)
            
        Returns:
            Si se proporciona ruta_imagen: ruta del PDF generado
            Si se proporciona contenido: bytes del PDF generado
        """
        if not HAS_PILLOW:
            logger.warning("No se puede convertir la imagen a PDF. Pillow no está instalado.")
            return ruta_imagen if ruta_imagen else None
            
        try:
            # Procesamiento desde archivo
            if ruta_imagen:
                # Obtener ruta base sin extensión y crear nueva ruta para PDF
                nombre_base, ext = os.path.splitext(ruta_imagen)
                ruta_pdf = f"{nombre_base}.pdf"
                
                # Abrir la imagen con Pillow
                imagen = Image.open(ruta_imagen)
                
                # Convertir a modo RGB si es necesario (RGBA, CMYK, etc.)
                if imagen.mode != 'RGB':
                    imagen = imagen.convert('RGB')
                    
                # Guardar como PDF
                imagen.save(ruta_pdf, "PDF", resolution=100.0)
                imagen.close()
                    
                # Eliminar archivo original si la conversión fue exitosa
                if os.path.exists(ruta_pdf):
                    os.remove(ruta_imagen)
                    logger.info(f"Imagen convertida a PDF: {ruta_pdf}")
                    return ruta_pdf
                else:
                    logger.error(f"No se pudo crear el PDF: {ruta_pdf}")
                    return ruta_imagen
            
            # Procesamiento desde bytes
            elif contenido:
                # Abrir la imagen desde bytes
                imagen = Image.open(io.BytesIO(contenido))
                
                # Convertir a modo RGB si es necesario
                if imagen.mode != 'RGB':
                    imagen = imagen.convert('RGB')
                
                # Guardar como PDF en un buffer de bytes
                pdf_buffer = io.BytesIO()
                imagen.save(pdf_buffer, "PDF", resolution=100.0)
                imagen.close()
                
                # Devolver los bytes del PDF
                pdf_bytes = pdf_buffer.getvalue()
                pdf_buffer.close()
                return pdf_bytes
            
            else:
                logger.error("No se proporcionó ni contenido ni ruta de imagen")
                return None
                
        except Exception as e:
            logger.error(f"Error al convertir imagen a PDF: {e}")
            return ruta_imagen if ruta_imagen else None
    
    def ejecutar(self):
        """Ejecuta la descarga y guarda el archivo, convirtiendo a PDF si es necesario"""
        try:
            # Asegurar que el directorio destino existe
            os.makedirs(os.path.dirname(self.ruta_destino), exist_ok=True)
            
            # Descargar contenido
            contenido = self.descargar_archivo()
            
            # Verificar si el contenido es una imagen
            es_imagen = self.es_imagen(contenido, self.content_type)
            
            # Definir ruta final
            ruta_final = self.ruta_destino
            
            # Asegurarse que la ruta final tenga extensión
            if not os.path.splitext(ruta_final)[1]:
                # Si no tiene extensión, añadir .pdf siempre
                ruta_final = f"{ruta_final}.pdf"
            
            # Procesar basado en el tipo de contenido
            if self.convertir_a_pdf and es_imagen:
                # Para imágenes, convertir directamente a PDF
                if HAS_PILLOW:
                    # Obtener los bytes del PDF desde la imagen
                    pdf_bytes = self.convertir_imagen_a_pdf(contenido=contenido)
                    
                    # Asegurar que la ruta final tenga extensión .pdf
                    nombre_base, ext = os.path.splitext(ruta_final)
                    if ext.lower() != '.pdf':
                        ruta_final = f"{nombre_base}.pdf"
                    
                    # Guardar PDF
                    if pdf_bytes:
                        with open(ruta_final, 'wb') as f:
                            f.write(pdf_bytes)
                        logger.info(f"Imagen convertida y guardada como PDF: {ruta_final}")
                    else:
                        # Si falla la conversión, guardar el archivo original
                        with open(ruta_final, 'wb') as f:
                            f.write(contenido)
                        logger.warning(f"No se pudo convertir la imagen. Guardado como: {ruta_final}")
                else:
                    # Si no está Pillow, guardar el archivo original
                    with open(ruta_final, 'wb') as f:
                        f.write(contenido)
                    logger.warning("No se puede convertir la imagen a PDF. Pillow no está instalado.")
            else:
                # Para otros tipos de archivo, guardar directamente
                with open(ruta_final, 'wb') as f:
                    f.write(contenido)
                    
            logger.info(f"Descarga exitosa: {ruta_final}")
            return True, ruta_final
        except Exception as e:
            logger.error(f"Error al procesar descarga {self.url}: {e}")
            return False, str(e)

class GestorDescargas:
    """
    Gestor de descargas múltiples con hilos
    """
    def __init__(self, max_workers=10):
        self.max_workers = max_workers
        self.cola_descargas = queue.Queue()
        self.resultados = []
        self.total_descargas = 0
        self.descargas_completadas = 0
        self.lock = threading.Lock()

    def agregar_descarga(self, url, ruta_destino, convertir_a_pdf=True):
        """Agrega una descarga a la cola
        
        Args:
            url: URL del archivo a descargar
            ruta_destino: Ruta donde guardar el archivo descargado
            convertir_a_pdf: Si es True, convierte imágenes a PDF automáticamente
        """
        self.cola_descargas.put((url, ruta_destino, convertir_a_pdf))
        self.total_descargas += 1

    def _proceso_descarga(self):
        """Proceso de descarga ejecutado en cada hilo"""
        while True:
            try:
                # Intentar obtener un trabajo de la cola
                url, ruta_destino, convertir_a_pdf = self.cola_descargas.get(block=False)
                
                # Procesar la descarga
                descargador = DescargadorArchivo(url, ruta_destino, convertir_a_pdf=convertir_a_pdf)
                resultado = descargador.ejecutar()
                
                # Actualizar contadores y resultados
                with self.lock:
                    self.descargas_completadas += 1
                    self.resultados.append({
                        'url': url,
                        'ruta': ruta_destino,
                        'exito': resultado[0],
                        'mensaje': resultado[1]
                    })
                
                # Marcar la tarea como completada
                self.cola_descargas.task_done()
                
            except queue.Empty:
                # No hay más trabajos en la cola
                break
            except Exception as e:
                logger.error(f"Error en proceso de descarga: {e}")
                with self.lock:
                    self.descargas_completadas += 1
                    self.resultados.append({
                        'url': url if 'url' in locals() else 'unknown',
                        'ruta': ruta_destino if 'ruta_destino' in locals() else 'unknown',
                        'exito': False,
                        'mensaje': str(e)
                    })
                if 'url' in locals():
                    self.cola_descargas.task_done()

    def ejecutar_descargas(self, callback_progreso=None):
        """
        Ejecuta todas las descargas en la cola usando múltiples hilos
        
        Args:
            callback_progreso: Función opcional para informar del progreso (recibe porcentaje)
        """
        if self.total_descargas == 0:
            logger.warning("No hay descargas para procesar")
            return self.resultados
            
        # Crear pool de hilos
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Iniciar hilos de trabajadores
            workers = [executor.submit(self._proceso_descarga) for _ in range(min(self.max_workers, self.total_descargas))]
            
            # Esperar a que todas las descargas estén completadas
            while self.descargas_completadas < self.total_descargas:
                time.sleep(0.1)
                if callback_progreso:
                    progreso = (self.descargas_completadas / self.total_descargas) * 100
                    callback_progreso(progreso)
            
        return self.resultados

    def obtener_resumen(self):
        """Devuelve un resumen de las descargas"""
        exitosas = sum(1 for r in self.resultados if r['exito'])
        fallidas = len(self.resultados) - exitosas
        
        return {
            'total': len(self.resultados),
            'exitosas': exitosas,
            'fallidas': fallidas,
            'porcentaje_exito': (exitosas / len(self.resultados) * 100) if self.resultados else 0
        }


class CombinadorPDF:
    """
    Clase para combinar múltiples PDFs en un solo archivo
    Con soporte para convertir automáticamente imágenes a PDF
    """
    def __init__(self):
        self.can_combine = HAS_PYPDF2
        self.can_convert = HAS_PILLOW
    
    def descargar_archivo(self, url):
        """
        Descarga un archivo desde una URL y lo devuelve como bytes junto con su tipo MIME
        """
        try:
            response = requests.get(url, timeout=30, stream=True)
            response.raise_for_status()  # Verificar si hay errores HTTP
            
            content_type = response.headers.get('Content-Type', '')
            
            # Verificar si es PDF o imagen para mensajes informativos
            if 'application/pdf' not in content_type and '.pdf' not in url.lower():
                # Si no es un PDF, verificar si es una imagen
                es_imagen = any(tipo in content_type for tipo in ['image/jpeg', 'image/png', 'image/gif', 'image/tiff'])
                if es_imagen:
                    logging.info(f"URL es una imagen que será convertida a PDF: {url}")
                else:
                    logging.warning(f"URL no parece ser un PDF ni una imagen reconocida: {url}, Content-Type: {content_type}")
                
            return response.content, content_type
        except Exception as e:
            logging.error(f"Error al descargar archivo desde {url}: {e}")
            return None, None
            
    def convertir_imagen_a_pdf_bytes(self, imagen_bytes, tipo_mime=None):
        """
        Convierte bytes de una imagen a bytes de un PDF
        
        Args:
            imagen_bytes: Bytes de la imagen original
            tipo_mime: Tipo MIME de la imagen (opcional)
            
        Returns:
            Bytes del PDF resultante o None si falla
        """
        if not self.can_convert:
            logging.warning("No se puede convertir la imagen a PDF. Pillow no está instalado.")
            return None
            
        try:
            # Intentar convertir directamente en memoria sin escribir a archivos temporales
            # Abrir la imagen desde bytes
            imagen = Image.open(io.BytesIO(imagen_bytes))
            
            # Convertir a modo RGB si es necesario (RGBA, CMYK, etc.)
            if imagen.mode != 'RGB':
                imagen = imagen.convert('RGB')
                
            # Guardar como PDF en un buffer de bytes
            pdf_buffer = io.BytesIO()
            imagen.save(pdf_buffer, "PDF", resolution=100.0)
            imagen.close()
            
            # Obtener los bytes del PDF
            pdf_bytes = pdf_buffer.getvalue()
            pdf_buffer.close()
            
            logging.info("Imagen convertida exitosamente a PDF en memoria")
            return pdf_bytes
                
        except Exception as e:
            logging.error(f"Error al convertir imagen a PDF: {e}")
            return None

    def es_imagen(self, contenido=None, tipo_mime=None):
        """
        Determina si el contenido o el Content-Type corresponde a una imagen
        
        Intenta detectar si el contenido es una imagen de dos formas:
        1. Comprobando si el tipo MIME indica que es una imagen
        2. Intentando abrir el contenido con PIL para confirmar
        
        Args:
            contenido: Bytes de la imagen (opcional)
            tipo_mime: Tipo MIME del contenido (opcional)
            
        Returns:
            True si es una imagen, False en caso contrario
        """
        # Verificar por tipo MIME
        if tipo_mime:
            if tipo_mime.startswith('image/'):
                return True
                
            # Mapeo adicional de tipos MIME
            tipos_imagen = ['image/jpeg', 'image/png', 'image/gif', 'image/tiff', 'image/bmp', 'image/webp']
            if any(tipo in tipo_mime for tipo in tipos_imagen):
                return True
                
        # Verificar intentando abrir con PIL si tenemos el contenido
        if contenido and self.can_convert:
            try:
                with Image.open(io.BytesIO(contenido)) as img:
                    return True
            except Exception:
                pass
                
        return False
    
    def combinar_pdfs(self, urls, ruta_destino):
        """
        Combina varios documentos descargados desde URLs en un solo archivo PDF
        Convierte automáticamente las imágenes a PDF antes de combinarlas
        
        Args:
            urls: Lista de URLs a descargar y combinar
            ruta_destino: Ruta donde guardar el PDF combinado
            
        Returns:
            Tupla (éxito, mensaje)
        """
        if not self.can_combine:
            return False, "PyPDF2 no está instalado. No se pueden combinar PDFs."
        
        if not urls:
            return False, "No se proporcionaron URLs para combinar"
        
        # Filtrar URLs vacías o None
        urls = [url for url in urls if url and isinstance(url, str)]
        if not urls:
            return False, "No hay URLs válidas para combinar"
            
        try:
            merger = PdfMerger()
            pdf_count = 0
            url_errors = []
            conversiones = 0
            
            # Descargar y combinar cada documento
            for url in urls:
                archivo_bytes, tipo_mime = self.descargar_archivo(url)
                if archivo_bytes:
                    try:
                        # Verificar si es una imagen y convertirla si es necesario
                        if self.es_imagen(contenido=archivo_bytes, tipo_mime=tipo_mime) and self.can_convert:
                            # Convertir imagen a PDF
                            pdf_bytes = self.convertir_imagen_a_pdf_bytes(archivo_bytes, tipo_mime)
                            if pdf_bytes:
                                archivo_bytes = pdf_bytes
                                conversiones += 1
                            else:
                                url_errors.append(f"No se pudo convertir la imagen a PDF: {url}")
                                continue
                                
                        # Intentar abrir como PDF
                        pdf_file = io.BytesIO(archivo_bytes)
                        try:
                            pdf_reader = PdfReader(pdf_file)
                            merger.append(pdf_reader)
                            pdf_count += 1
                        except Exception as e:
                            url_errors.append(f"Error al procesar como PDF: {url}: {str(e)}")
                            logging.error(f"Error al procesar PDF de {url}: {e}")
                    except Exception as e:
                        url_errors.append(f"Error al procesar {url}: {str(e)}")
                        logging.error(f"Error al procesar archivo de {url}: {e}")
                else:
                    url_errors.append(f"Error al descargar {url}")
            
            # Verificar si se combinó al menos un PDF
            if pdf_count == 0:
                return False, f"No se pudieron combinar documentos. Errores: {', '.join(url_errors)}"
            
            # Guardar el PDF combinado
            directorio = os.path.dirname(ruta_destino)
            os.makedirs(directorio, exist_ok=True)
            
            merger.write(ruta_destino)
            merger.close()
            
            # Mensaje de resultados
            mensaje_base = f"PDF combinado exitosamente con {pdf_count} documentos"
            if conversiones > 0:
                mensaje_base += f" ({conversiones} imágenes convertidas)"
            
            # Verificar si hubo errores parciales
            if url_errors:
                mensaje = f"{mensaje_base} (hubo {len(url_errors)} errores)"
                return True, mensaje
            else:
                return True, mensaje_base
                
        except Exception as e:
            return False, f"Error al combinar documentos: {str(e)}"


def preparar_ruta_destino(directorio_base, nombre_archivo, prefijo=None):
    """
    Prepara una ruta de destino para guardar un archivo descargado
    
    Args:
        directorio_base: Directorio donde se guardarán las descargas
        nombre_archivo: Nombre del archivo a descargar
        prefijo: Prefijo opcional para añadir al nombre del archivo
    """
    # Crear directorio si no existe
    Path(directorio_base).mkdir(parents=True, exist_ok=True)
    
    # Sanitizar nombre de archivo
    nombre_limpio = "".join(c for c in nombre_archivo if c.isalnum() or c in (' ', '.', '_', '-'))
    nombre_limpio = nombre_limpio.replace(' ', '_')
    
    # Añadir prefijo si existe
    if prefijo:
        nombre_limpio = f"{prefijo}_{nombre_limpio}"
    
    return os.path.join(directorio_base, nombre_limpio)


def sanitizar_nombre_archivo(texto):
    """
    Sanitiza un texto para usarlo como nombre de archivo
    """
    if not texto:
        return f"doc_{str(uuid.uuid4())[:8]}"
        
    # Reemplazar caracteres no permitidos
    nombre_limpio = "".join(c for c in texto if c.isalnum() or c in (' ', '.', '_', '-'))
    nombre_limpio = nombre_limpio.replace(' ', '_')
    
    # Si después de sanitizar queda vacío, usar un nombre genérico
    if not nombre_limpio:
        nombre_limpio = f"doc_{str(uuid.uuid4())[:8]}"
        
    return nombre_limpio
