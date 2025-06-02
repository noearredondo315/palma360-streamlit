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
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
import mimetypes
import logging
import uuid

# Importar pikepdf para combinar PDFs (más rápido y eficiente que PyPDF2)
try:
    import pikepdf
    HAS_PIKEPDF = True
except ImportError:
    HAS_PIKEPDF = False
    logging.warning("pikepdf no está instalado. La funcionalidad de combinación de PDFs no estará disponible.")
    logging.warning("Para instalarlo, ejecuta: pip install pikepdf")

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

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    def descargar_archivo(self):
        """Descarga un archivo desde una URL con reintentos"""
        try:
            # Usar una sesión global compartida para reutilizar conexiones
            if not hasattr(DescargadorArchivo, '_session'):
                DescargadorArchivo._session = requests.Session()
                # Configurar la sesión para reutilizar conexiones
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=20,
                    pool_maxsize=20,
                    max_retries=0  # Los reintentos los manejamos con tenacity
                )
                DescargadorArchivo._session.mount('http://', adapter)
                DescargadorArchivo._session.mount('https://', adapter)
            
            response = DescargadorArchivo._session.get(self.url, timeout=10, stream=True)
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
        # Verificar por tipo MIME - normalizar a minúsculas para mejorar detección
        if content_type:
            content_type = content_type.lower()
            if content_type.startswith('image/'):
                return True
                
            # Mapeo mejorado de tipos MIME para imágenes
            mime_map_images = [
                'image/jpeg', 'image/jpg', 'image/png', 'image/gif', 'image/bmp', 
                'image/tiff', 'image/webp', 'image/x-icon', 'image/svg+xml'
            ]
            if any(mime in content_type for mime in mime_map_images):
                return True
        
        # Verificar por extensión en la URL
        if hasattr(self, 'url') and self.url:
            url_lower = self.url.lower()
            if any(ext in url_lower for ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp']):
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
    def __init__(self, max_workers=None):
        # Configurar el número de workers basado en CPUs disponibles si no se especifica
        if max_workers is None:
            # Usar min para evitar crear demasiados hilos en sistemas con muchos núcleos
            self.max_workers = min(multiprocessing.cpu_count() * 2, 20)
        else:
            self.max_workers = max_workers
            
        self.cola_descargas = queue.Queue()
        self.resultados = []
        self.total_descargas = 0
        self.descargas_completadas = 0
        self.lock = threading.Lock()
        self.session = requests.Session()  # Sesión compartida para todas las descargas

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

    def ejecutar_descargas(self, callback_progreso=None, limite_ancho_banda_kb=None):
        """
        Ejecuta todas las descargas en la cola usando múltiples hilos
        
        Args:
            callback_progreso: Función opcional para informar del progreso (recibe porcentaje)
            limite_ancho_banda_kb: Límite opcional de ancho de banda en KB/s
        """
        if self.total_descargas == 0:
            logger.warning("No hay descargas para procesar")
            return self.resultados
        
        # Almacenar el límite de ancho de banda si se proporciona
        self.limite_ancho_banda_kb = limite_ancho_banda_kb
        
        # Crear pool de hilos con el número apropiado de workers
        with ThreadPoolExecutor(max_workers=min(self.max_workers, self.total_descargas)) as executor:
            # Preparar las tareas
            futures = []
            for _ in range(min(self.max_workers, self.total_descargas)):
                futures.append(executor.submit(self._proceso_descarga))
            
            # Usar as_completed para procesar los resultados a medida que terminan
            # y evitar el polling con time.sleep
            for future in as_completed(futures):
                try:
                    future.result()  # Obtener resultado o excepción
                except Exception as e:
                    logger.error(f"Error en hilo de descarga: {e}")
                
                if callback_progreso:
                    with self.lock:
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
    Usa pikepdf para mayor rendimiento
    """
    def __init__(self):
        self.can_combine = HAS_PIKEPDF
        self.can_convert = HAS_PILLOW
        # Crear una sesión compartida para todas las descargas
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=0
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
    
    def descargar_archivo(self, url):
        """
        Descarga un archivo desde una URL y lo devuelve como bytes junto con su tipo MIME
        Usa la sesión compartida para mejorar rendimiento
        """
        try:
            # Usar la sesión compartida
            response = self.session.get(url, timeout=10, stream=True)
            response.raise_for_status()  # Verificar si hay errores HTTP
            
            content_type = response.headers.get('Content-Type', '').lower()
            
            # Verificar si es PDF o imagen para mensajes informativos
            if 'application/pdf' not in content_type and '.pdf' not in url.lower():
                # Determinar si es una imagen con detección mejorada
                mime_images = ['image/jpeg', 'image/jpg', 'image/png', 'image/gif', 'image/tiff', 'image/bmp', 'image/webp']
                ext_images = ['.jpg', '.jpeg', '.png', '.gif', '.tiff', '.tif', '.bmp', '.webp']
                
                es_imagen_mime = any(tipo in content_type for tipo in mime_images)
                es_imagen_ext = any(ext in url.lower() for ext in ext_images)
                
                if es_imagen_mime or es_imagen_ext:
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
    
    def combinar_pdfs_a_memoria(self, urls):
        """
        Combina varios documentos descargados desde URLs en un solo archivo PDF en memoria
        Convierte automáticamente las imágenes a PDF antes de combinarlas
        Usa pikepdf para mejor rendimiento y menor consumo de memoria
        
        Args:
            urls: Lista de URLs a descargar y combinar
            
        Returns:
            Tupla (éxito, mensaje, bytes_del_pdf)
        """
        if not self.can_combine:
            return False, "pikepdf no está instalado. No se pueden combinar PDFs.", None
        
        if not urls:
            return False, "No se proporcionaron URLs para combinar", None
        
        # Filtrar URLs vacías o None
        urls = [url for url in urls if url and isinstance(url, str)]
        if not urls:
            return False, "No hay URLs válidas para combinar", None
            
        try:
            pdf_docs = []  # Lista para almacenar documentos PDF temporales
            pdf_count = 0
            url_errors = []
            conversiones = 0
            
            # Descargar y preparar cada documento
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
                                
                        # Cargar PDF en memoria y añadirlo a la lista
                        try:
                            # Usar memoryview para evitar copias innecesarias de datos
                            pdf_file = io.BytesIO(archivo_bytes)
                            pdf = pikepdf.Pdf.open(pdf_file)
                            pdf_docs.append(pdf)
                            pdf_count += 1
                        except Exception as e:
                            url_errors.append(f"Error al procesar como PDF: {url}: {str(e)}")
                            logging.error(f"Error al procesar PDF de {url}: {e}")
                    except Exception as e:
                        url_errors.append(f"Error al procesar {url}: {str(e)}")
                        logging.error(f"Error al procesar archivo de {url}: {e}")
                else:
                    url_errors.append(f"Error al descargar {url}")
            
            if pdf_count == 0:
                return False, f"No se pudieron combinar documentos. Errores: {', '.join(url_errors)}", None
            
            # Combinar PDFs con pikepdf (más eficiente que PyPDF2)
            pdf_final = pikepdf.Pdf.new()
            
            for pdf in pdf_docs:
                pdf_final.pages.extend(pdf.pages)
            
            # Guardar el PDF combinado en memoria en lugar de en disco
            pdf_bytes = io.BytesIO()
            pdf_final.save(pdf_bytes)
            pdf_bytes.seek(0)  # Rebobinar al principio para leer después
            resultado_bytes = pdf_bytes.read()
            
            # Cerrar todos los PDF abiertos para liberar memoria
            for pdf in pdf_docs:
                pdf.close()
            pdf_final.close()
            
            # Mensaje de resultados
            mensaje_base = f"PDF combinado exitosamente con {pdf_count} documentos"
            if conversiones > 0:
                mensaje_base += f" ({conversiones} imágenes convertidas)"
            
            # Verificar si hubo errores parciales
            if url_errors:
                mensaje = f"{mensaje_base} (hubo {len(url_errors)} errores)"
                return True, mensaje, resultado_bytes
            else:
                return True, mensaje_base, resultado_bytes
                
        except Exception as e:
            return False, f"Error al combinar documentos: {str(e)}", None

    def combinar_pdfs(self, urls, ruta_destino):
        """
        Combina varios documentos descargados desde URLs en un solo archivo PDF
        Convierte automáticamente las imágenes a PDF antes de combinarlas
        Usa pikepdf para mejor rendimiento y menor consumo de memoria
        
        Args:
            urls: Lista de URLs a descargar y combinar
            ruta_destino: Ruta donde guardar el PDF combinado
            
        Returns:
            Tupla (éxito, mensaje)
        """
        exito, mensaje, pdf_bytes = self.combinar_pdfs_a_memoria(urls)
        
        if not exito or not pdf_bytes:
            return exito, mensaje
            
        try:
            # Guardar el PDF combinado en disco
            directorio = os.path.dirname(ruta_destino)
            os.makedirs(directorio, exist_ok=True)
            
            with open(ruta_destino, 'wb') as f:
                f.write(pdf_bytes)
            
            return True, mensaje
        except Exception as e:
            return False, f"Error al guardar el PDF: {str(e)}"

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
