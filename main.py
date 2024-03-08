import configparser
import datetime
import json
import logging.config
import os
import pathlib
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from multiprocessing import Pool
from queue import Queue

import requests
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from requests.auth import HTTPBasicAuth

import filters

archivos_completados = 0


def isRunning(ES_HOST, ES_USER, ES_PASSWORD):
    """
        Comprueba si se puede establecer una conexión con un servidor de Elasticsearch.

        Args:
        ES_HOST (str): URL del servidor de Elasticsearch.
        ES_USER (str): Nombre de usuario para la autenticación.
        ES_PASSWORD (str): Contraseña para la autenticación.

        Returns:
        bool: True si se establece una conexión exitosa, False en caso contrario.

    """
    try:
        res = requests.get(
            url=ES_HOST,
            auth=HTTPBasicAuth(username=ES_USER, password=ES_PASSWORD),
            verify=False,
        )
        if res.status_code == 200:
            LOGGER.debug(
                f"[IS_RUNNING]: Conexión establecida con Elasticsearch")
            return True

        return False
    except Exception as e:
        LOGGER.error(f"[IS_RUNNING] - Ha ocurrido un error: {e}")
        return False


def fileWatcher():
    """
    Función que vigila un directorio específico y controla el estado de los archivos en el mismo.
    Si no hay archivos nuevos, se detiene el script.
    Si hay archivos nuevos, se envían a una cola para su procesamiento.

    Args:
    None

    Returns:
    None
    """
    try:
        global stop_threads, config
        watchDirectory = config["WATCHER_FILE"]["WATCH_DIRECTORY"]
        data = []
        dir_bp = os.listdir("dir_backup")

        LOGGER.info(f"[WATCHER] - Carpeta 'dir_backup': {dir_bp}")
        dict_temp = {}

        if not dir_bp:  # Si no hay nada en "dir_backup", crea un archivo nuevo
            with open(
                "dir_backup/dir_backup.json",
                "w+",
                errors="replace",
                encoding="us_ascii",
            ) as file_bp:
                previousFileList = fileInDirectory(watchDirectory)
                for i in previousFileList:
                    dict_temp[pathlib.Path(i).as_posix()] = {
                        "total_lineas": numLineasFichero(i),
                        "linea_actual": 0,
                        "estado": "En curso",
                    }

                file_bp.write(
                    json.dumps(dict_temp, indent=4) + "\n"
                )  # Escribe en el JSON los archivos que se leerán

        else:
            with open(
                "dir_backup/dir_backup.json",
                "r+",
                errors="replace",
                encoding="us_ascii",
            ) as backup:
                backup.seek(0)
                first_char = backup.read(1)

                if not first_char:
                    LOGGER.warning(
                        f"[WATCHER] - El archivo '{backup.name}' está vacio. Procesando..."
                    )
                    os.remove(backup.name)
                    fileWatcher()
                else:
                    backup.seek(0)
                    previousFileList = fileInDirectory(watchDirectory)
                    data.extend(previousFileList)
                    datos = json.load(backup)

                    """
                        Si existe el archivo, verificará si los archivos estan completados o no.
                    """

                    for element in data:
                        if element not in datos.keys():
                            dict_temp[pathlib.Path(element).as_posix()] = {
                                "total_lineas": numLineasFichero(element),
                                "linea_actual": 0,
                                "estado": "En curso",
                            }
                            data.append(element)
                            datos.update(dict_temp)

                        else:
                            if datos[element]["estado"] == "En curso":
                                global linea_actual
                                linea_actual = datos[element]["linea_actual"]
                                dict_temp[pathlib.Path(element).as_posix()] = {
                                    "total_lineas": numLineasFichero(element),
                                    "linea_actual": linea_actual,
                                    "estado": "En curso",
                                }

                            elif datos[element]["estado"] == "Completado":
                                global archivos_completados
                                archivos_completados = archivos_completados + 1
                                # continue
                    backup.seek(0)
                    backup.write(json.dumps(datos, indent=4) + "\n")
                    backup.truncate()
        if not dict_temp.copy():  # Si no hay valores, se detiene el script
            stop_threads = True  # Se detienen todos los hilos
            LOGGER.info(
                "[WATCHER] - No hay archivos nuevos. Programa finalizado.")
            stop_threads = True
            os._exit(0)
        else:  # Si hay valores se envía a cola
            QUEUE_WATCHER.put(dict_temp.copy())
            QUEUE_WATCHER.task_done()

    except Exception as e:
        if "i" in locals():
            LOGGER.error(
                f"[WATCHER] - Ha ocurrido un error: {e}, Archivo: {i}")
            os._exit(1)
        else:
            LOGGER.error(f"[WATCHER] - Ha ocurrido un error: {e}")
            os._exit(1)


def listComparison(OriginalList: list, NewList: list):
    """
    Compara dos listas y devuelve una lista con los elementos que están en la lista `NewList` pero no en la lista `OriginalList`.

    Args:
        OriginalList (list): La lista original con la que se comparará.
        NewList (list): La lista que se comparará con la lista original.

    Returns:
        list: Una lista de los elementos que están en `NewList` pero no en `OriginalList`.

    Raises:
        None.

    Examples:
        listComparison([1, 2, 3], [2, 3, 4]) => [4]
    """
    try:
        differencesList = [x for x in NewList if x not in OriginalList]
        if differencesList:
            print("Se han encontrado archivos nuevos: ", differencesList)
        return differencesList
    except Exception as e:
        LOGGER.error(f"[LIST_COMPARISON] - Ha ocurrido un error: {e}")
        os._exit(1)


def fileInDirectory(my_dir: str):
    """
    Devuelve una lista de rutas de archivos que se encuentran dentro de un directorio (y sus subdirectorios) que coinciden con la extensión ".DAT".

    Args:
    my_dir (str): Ruta del directorio a explorar.

    Returns:
    List[str]: Lista de rutas de archivos con extensión ".DAT" que se encuentran dentro del directorio especificado.
    """

    global cant_archivos
    only = []
    for p in pathlib.Path(my_dir).rglob("*.DAT"):
        only.append(pathlib.Path(p).as_posix())
    cant_archivos = len(only)
    return only


def numLineasFichero(fichero):
    """
    Función que recibe como parámetro el nombre de un archivo y retorna la cantidad de líneas que contiene. En caso de que no se pueda abrir el archivo, se mostrará un mensaje de error y se retornará -1.

    Args:
    - fichero: nombre del archivo a leer.

    Returns
    - cantidad de líneas que contiene el archivo.

    Raises:
    - AttributeError: se produce si no se proporciona un nombre de archivo válido.
    """

    try:
        with open(fichero, "r", errors="replace", encoding="us_ascii") as archivo:
            archivo.seek(0)
            return len(archivo.readlines())
    except AttributeError:
        LOGGER.error(f'[NUMLINEAS] - Se debe insertar un fichero en "/source"')
        return -1


def lectura(name_file):
    """Lee un archivo y desglosa su contenido en líneas individuales para su posterior procesamiento y transformación.

    Args:
        name_file (dict): Un diccionario que contiene información sobre el archivo que se va a leer, incluyendo su ubicación, el número total de líneas en el archivo y la línea actual en la que se detuvo la lectura anterior.

    Returns:
        None

    Raises:
        Exception: Si se produce algún error al leer el archivo.

    """

    global total_documentos, archivo_trabajado, listaArchivosTotales, linea_de_archivos
    keys = list(name_file.keys())
    total_documentos = [name_file[i]["total_lineas"] for i in keys]

    for i in keys:
        index = 0
        linea_actual = 0
        try:
            with open(i, "r", encoding="utf-8", errors="replace") as doc:
                documentos = []

                # total_documentos.append(name_file[i]["total_lineas"])
                LOGGER.debug("[READ FILE] - Leyendo...")
                LOGGER.debug("[READ FILE] - Documentos añadidos a la cola...")
                archivo_trabajado = name_file[i]
                linea_actual = name_file[i]["linea_actual"]
                dat = doc.readlines()[linea_actual:]
                for linea in dat:
                    linea.encode("utf-8")
                    documentos.append(
                        {
                            "linea": linea,
                            "path": i,
                            "linea actual": linea_actual,
                            "total_lineas": name_file[i]["total_lineas"],
                        }
                    )

                    linea_actual = linea_actual + 1

                    if len(documentos) - LIMIT_QUEUE == 0:
                        QUEUE_READ.put(documentos.copy())
                        name_file[i]["linea_actual"] = linea_actual
                        LOGGER.debug(
                            "Documentos %s añadidos a la cola.", len(
                                documentos.copy())
                        )
                        documentos = []

                if documentos:
                    QUEUE_READ.put(documentos.copy())
                    name_file[i]["linea_actual"] = linea_actual
                    LOGGER.debug(
                        "Documentos %s añadidos a la cola.", len(
                            documentos.copy())
                    )
                    documentos = []
            LOGGER.debug("[READ FILE] - Finalizado")
        except Exception as e:
            LOGGER.error(f"[LECTURA] - Ha ocurrido un error: {e}")
            os._exit(1)


def read_file():
    """Desglosa un archivo para el futuro procesamiento y transformación."""
    global cant_archivos, config, watcher

    watcher.join()
    if stop_threads:
        os._exit(0)
    else:
        num_process_read = int(config["PROCESS_READ_FILE"]["NUM_PROCESS_READ"])
        data = []
        try:
            while True:
                LOGGER.debug("[READ FILE] - Leyendo directorios...")
                data.append(QUEUE_WATCHER.get())
                if len(data[0]) == (cant_archivos - archivos_completados):
                    with ThreadPoolExecutor(num_process_read) as executor:
                        # futures = [executor.submit(lectura, f) for f in data]
                        futures = executor.submit(lectura, data[0])
                        # for future in concurrent.futures.as_completed(futures):
                        #     try:
                        #         data = future.result()
                        #     except Exception as e:
                        #         print("Se ha generado una excepcion: %s", e)
                        #     else:
                        #         print(f"Datos: {data}")
                break
            # read.join()
        except queue.Empty:
            pass

        except Exception as e:
            LOGGER.error(f"[READ_FILE] - Ha ocurrido un error: {e}")
            os._exit(1)


def transform_data():
    """
    Transforma los datos de la cola de entrada utilizando múltiples procesos, y coloca los resultados en la cola de salida.
    Los datos se procesan utilizando la función `filters.process_data`.

    Args:
        None

    Returns:
        None

    Raises:
        OSError: Si se produce un error al intentar salir del proceso.
        Exception: Si se produce un error inesperado durante el procesamiento de los datos.

    """

    global config
    try:
        while True:
            if stop_threads:
                os._exit(0)
            else:
                LOGGER.debug("[TRANSFORM DATA]")
                data = QUEUE_READ.get(timeout=180)
                pool = Pool(
                    processes=int(
                        config["PROCESS_TRANSFORM_FILE"]["NUM_PROCESS_TRANSFORM"]
                    )
                )
                map = pool.map(filters.process_data, data)
                pool.close()
                pool.join()

                QUEUE_OUTPUT.put(map)

                QUEUE_READ.task_done()
                LOGGER.debug("[TRANSFORM DATA] - Completed")

    except queue.Empty:
        pass
    except Exception as e:
        LOGGER.error(f"[TRANSFORM] - Ha ocurrido un error: {e}")
        os._exit(1)


def write_dir_backup(ruta, ult_linea):
    """
    Actualiza el archivo de backup de un directorio dado con la línea actual y el estado del proceso de escritura.

    Args:
        - ruta (str): Ruta del directorio para el cual se actualizará el archivo de backup.
        - ult_linea (int): Última línea escrita del archivo correspondiente al directorio.

    Returns:
        None
    """

    with open("dir_backup/dir_backup.json", "r+", errors="replace") as dir_bp:
        data = json.load(dir_bp)
        data[ruta]["linea_actual"] = ult_linea
        LOGGER.debug(
            f'[WRITE_DIR_BACKUP] - Documentos Escritos {data[ruta]["linea_actual"]}'
        )

        if data[ruta]["linea_actual"] == data[ruta]["total_lineas"]:
            data[ruta]["estado"] = "Completado"
            LOGGER.info(f'[OUTPUT] - Archivo "{ruta}" completado.')
        dir_bp.seek(0)
        json.dump(data, dir_bp, indent=4)
        dir_bp.truncate()


def output_data():
    """
    Esta función recibe datos desde una cola, los procesa y los envía a dos instancias diferentes de Elasticsearch para su
    indexación. Una vez finalizado el proceso, informa el número total de documentos indexados, el número de duplicados y
    el tiempo que tomó la ejecución del proceso.

    Args:
        None

    Returns:
        None
    """

    global duplicados, linea_actual, totals, stop_threads, inicio, dupl_1, dupl_2
    ult_id = ""

    try:
        LOGGER.debug("[OUTPUT] - Starting...")

        try:
            while True:
                aprobados = 0
                apr_1 = 0
                apr_2 = 0

                get_id = []
                #datos = QUEUE_OUTPUT.get()
                data = QUEUE_OUTPUT.get(timeout=180)
                QUEUE_OUTPUT.task_done()
                datos = []
                #if not datos:
                #    output_data()
                #for i in datos:
                #    get_id.append(i.get("_id"))
                if not data:
                    output_data()
                for i in data:
                    if i != None:
                        datos.append(i)
                        get_id.append(i.get("_id"))
                id_confirm_1 = ""
                id_confirm_2 = ""
                ult_id = datos[-1]["_id"]
                ult_linea = datos[-1]["linea_actual"] + 1
                path = datos[-1]["path"]

                for linea in range(len(datos)):
                    datos[linea].pop("path")
                    datos[linea].pop("linea_actual")
                    datos[linea].pop("total_lineas")

                datos_id = []

                for ok, action in helpers.parallel_bulk(
                        ES_CLI, datos, index=ES_IDX, thread_count=2
                ):
                    if ok:
                        if action["index"]["result"] == "updated":
                            apr_1 = apr_1 + 1
                            dupl_1 = dupl_1 + 1
                            LOGGER.debug(
                                f"[OUTPUT](Document id {action['index']['_id']} already exists, hay {duplicados} duplicados. >> Indice: {action['index']['_index']}"
                            )
                            id_confirm_1 = action["index"]["_id"]
                            datos_id.append(action["index"]["_id"])
                        else:
                            apr_1 = apr_1 + 1
                            id_confirm_1 = action["index"]["_id"]
                    else:
                        print(action)
                for ok, action in helpers.parallel_bulk(
                        ES_CLI_2, datos, index=ES_IDX, thread_count=2
                ):
                    if ok:
                        if action["index"]["result"] == "updated":
                            apr_2 = apr_2 + 1
                            dupl_2 = dupl_2 + 1
                            LOGGER.debug(
                                f"[OUTPUT](Document id {action['index']['_id']} already exists, hay {duplicados} duplicados. >> Indice: {action['index']['_index']}"
                            )
                            id_confirm_2 = action["index"]["_id"]
                            datos_id.append(action["index"]["_id"])
                        else:
                            apr_2 = apr_2 + 1
                            id_confirm_2 = action["index"]["_id"]

                    else:
                        print(action)

                if apr_2 == apr_1:
                    aprobados = apr_1
                if dupl_1 == dupl_2:
                    duplicados = dupl_1
                if id_confirm_1 == id_confirm_2:
                    id_confirm = id_confirm_1

                COMPLETADOS.append(aprobados)

                # Suma acumulada de docs completados
                total_total = sum(total_documentos)
                datos_completados = sum(COMPLETADOS)

                if "linea_actual" in globals():
                    totals = total_total - linea_actual
                    if datos_completados < totals:
                        if id_confirm == ult_id:
                            write_dir_backup(path, ult_linea)
                        LOGGER.info(
                            f"[OUTPUT] - Documentos subidos: {str(datos_completados)}, Duplicados: {str(duplicados)} de {str(total_total)}  - Archivo actual: {str(path)}  >> Indice: {action['index']['_index']}. Destino [{ES_HST}, {ES_HST_1}]"
                        )
                        del linea_actual
                    else:
                        total = datos_completados - duplicados
                        if id_confirm == ult_id:
                            write_dir_backup(path, ult_linea)

                        LOGGER.info(
                            f"[OUTPUT] - Se subieron {str(total)} de {str(total_documentos)} - Duplicados: {str(duplicados)} Total registro: {str(datos_completados)} Archivo {str(path)} completado.  >> Indice: {action['index']['_index']}"
                        )

                        stop_threads = True
                        del linea_actual  # Se detienen todos los hilos

                        final = time.time()
                        segundos = (final - inicio)
                        horas = int(segundos/60/60)
                        segundos -= horas*60*60
                        minutos = int(segundos/60)
                        segundos -= minutos*60

                        tiempos = "{:.0f} horas, {:.0f} minutos, {:.02f} segundos".format(
                            horas, minutos, segundos)

                        LOGGER.info(
                            f"[OUTPUT] - Tiempo transcurrido: {tiempos}"
                        )
                        LOGGER.info(
                            f"-----------   Programa Finalizado   -------------"
                        )
                        stop_threads = True  # Se detienen todos los hilos

                        os._exit(0)
                else:
                    if datos_completados < totals:
                        LOGGER.info(
                            f"[OUTPUT] - Documentos subidos: {str(datos_completados)}, Duplicados: {str(duplicados)} de {str(total_total)} - Archivo actual: {str(path)} >> Indice: {action['index']['_index']}. Destino [{ES_HST}, {ES_HST_1}]"
                        )
                        if id_confirm == ult_id:
                            write_dir_backup(path, ult_linea)
                    else:
                        total = datos_completados - duplicados
                        if id_confirm == ult_id:
                            write_dir_backup(path, ult_linea)
                        LOGGER.info(
                            f"[OUTPUT] - Se subieron {str(total)} de {str(total_total)} - Duplicados: {str(duplicados)} Total registro: {str(datos_completados)}"
                        )

                        final = time.time()
                        segundos = (final - inicio)
                        horas = int(segundos/60/60)
                        segundos -= horas*60*60
                        minutos = int(segundos/60)
                        segundos -= minutos*60

                        tiempos = "{:.0f} horas, {:.0f} minutos, {:.02f} segundos".format(
                            horas, minutos, segundos)

                        LOGGER.info(
                            f"[OUTPUT] - Tiempo transcurrido: {tiempos}"
                        )
                        LOGGER.info(
                            f"--------------------------------   Programa Finalizado   --------------------------------"
                        )
                        stop_threads = True  # Se detienen todos los hilos

                        os._exit(0)  # Finaliza el programa

        except helpers.BulkIndexError as e:
            for error in e.errors:
                if error["create"]["status"] == 409:  # ERROR 409: Docs Duplicados
                    duplicados = duplicados + 1
                    LOGGER.debug(
                        f"Document id {error['create']['_id']} already exists, hay {duplicados} duplicados."
                    )
                    continue

        except Exception as e:
            LOGGER.error(f"[OUTPUT] - Ha ocurrido un error: {e}")
            os._exit(0)

        LOGGER.debug("[OUTPUT] - Completed")

    except Exception as e:
        LOGGER.error("[OUTPUT] - Ha ocurrido un error: {e}")
        os._exit(0)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")

    logging.config.fileConfig("./logger.ini")
    LOGGER = logging.getLogger(__name__)
    hoy = date.today()
    fh = logging.FileHandler(f"logs/log-file-{hoy}.log")
    formatter = logging.Formatter(
        "%(asctime)s :: %(name)-10s :: %(filename)s:%(lineno)d :: %(levelname)-10s :: %(message)-10s"
    )
    fh.setFormatter(formatter)
    LOGGER.addHandler(fh)

    logging.getLogger("elastic_transport.transport").setLevel(logging.INFO)

    if (config["LOGGER"]["PROPAGATE"]) == "True":
        propagacion = True
    elif config["LOGGER"]["PROPAGATE"] == "False":
        propagacion = False

    LOGGER.propagate = propagacion
    LOGGER.info(
        f"-----------   Programa Iniciado   ---------------------"
    )

    QUEUE_WATCHER = Queue(int(config["QUEUE_CONFIG"]["QUEUE_WATCHER"]))
    QUEUE_READ = Queue(int(config["QUEUE_CONFIG"]["QUEUE_READ"]))
    QUEUE_OUTPUT = Queue(int(config["QUEUE_CONFIG"]["QUEUE_OUTPUT"]))
    QUEUE_LINE = Queue(int(config["QUEUE_CONFIG"]["QUEUE_LINE"]))
    LIMIT_QUEUE = int(config["QUEUE_CONFIG"]["LIMIT_QUEUE_DATA"])

    ES_USR = config["CONFIG_ES"]["ES_USER"]  # Usuario Elastic
    ES_PDW = config["CONFIG_ES"]["ES_PASSWORD"]  # Password Elastic
    ES_HST = config["CONFIG_ES"]["ES_HOST"]  # Host Elastic
    ES_HST_1 = config["CONFIG_ES"]["ES_HOST_1"]  # Host Elastic
    AÑO = datetime.date.today().strftime("%Y")
    ES_IDX = config["CONFIG_ES"]["ES_IDX"]  # Indice Elastic

    ES_CLI = Elasticsearch(
        [ES_HST],
        http_auth=(ES_USR, ES_PDW),
        request_timeout=1000,
        retry_on_timeout=True,
        verify_certs=False,
        ssl_show_warn=False,
    )
    ES_CLI_2 = Elasticsearch(
        [ES_HST_1],
        http_auth=(ES_USR, ES_PDW),
        request_timeout=1000,
        retry_on_timeout=True,
        verify_certs=False,
        ssl_show_warn=False,
    )

    COMPLETADOS = []
    inicio = time.time()
    duplicados = 0
    dupl_1 = 0
    dupl_2 = 0
    total_documentos = []
    linea_de_archivos = {}
    archivo_trabajado = ""
    cant_archivos = 0
    linea_actual = 0
    totals = 0
    stop_threads = False

    intento = 0

    if isRunning(ES_HST, ES_USR, ES_PDW) and isRunning(ES_HST_1, ES_USR, ES_PDW):
        if stop_threads:
            os._exit(0)
        else:
            for i in range(1):
                watcher = threading.Thread(target=fileWatcher, name="watcher")
                watcher.start()
            for i in range(int(config["THREADS_READ"]["NUM_THREADS_READ"])):
                read = threading.Thread(target=read_file, name="read")
                read.start()

            for i in range(int(config["THREADS_TRANSFORM"]["NUM_THREADS_TRANSFORM"])):
                transform = threading.Thread(
                    target=transform_data, name="Transform")
                transform.start()

            for i in range(1):
                salida = threading.Thread(target=output_data, name="salida")
                salida.start()

    else:
        while True:
            for i in range(3):
                LOGGER.info(
                    f"No hay conexion... Reintentando - Intento {intento + 1}")
                time.sleep(2)
                isRunning(ES_HST, ES_USR, ES_PDW)
                isRunning(ES_HST_1, ES_USR, ES_PDW)
                intento = intento + 1
            if intento == 3:
                LOGGER.error("No se pudo conectar.")

                break
