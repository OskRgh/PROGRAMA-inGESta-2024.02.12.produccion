import configparser
import datetime
import json
import logging.config
from collections import defaultdict
from datetime import date, datetime, timedelta

import dateutil

import fingerprint
from structure_data import estructura

"""Inicializacion y configuracion de logging"""

config = configparser.ConfigParser()
config.read("config.ini")

# LEVEL_LOGGER = config["LOGGER"]["LEVEL_LOGGER"]


# logging.basicConfig(
#         filename="logs/log-file-" + str(date.today()) + ".log",
#         filemode="a",
#         format="%(asctime)-10s :: %(name)-10s :: %(levelname)-10s :: \n%(message)30s",
#         level= logging.INFO
# )
#
# LOGGER = logging.getLogger(__name__)
# logging.getLogger("elastic_transport.transport").setLevel(logging.INFO)


logging.config.fileConfig("./logger.ini")
LOGGER = logging.getLogger(__name__)
logging.getLogger("elastic_transport.transport").setLevel(logging.DEBUG)
hoy = date.today()
fh = logging.FileHandler(f'logs/log-file-{hoy}.log')
formatter = logging.Formatter('%(asctime)s :: %(name)-10s :: %(levelname)-10s :: %(message)-10s')
fh.setFormatter(formatter)
LOGGER.addHandler(fh)

AÑO = date.today().strftime("%Y")
ES_IDX = config["CONFIG_ES"]["ES_IDX"]


def dictt():
    """Crea un objeto de tipo 'Default dict'."""

    return defaultdict(dictt)


TEMP = dictt()


def process_data(item):
    """
    Procesamiento de datos en distintas etapas.

    Return:
        Devuelve un Iterator con la info estructurada.

    """
    try:
        LOGGER.debug(f"[PROCESS DATA] - Procesando datos... - Archivo: {item['path']}")

        LOGGER.debug(f"[PROCESS DATA] - Ejecutando 'estructura' - Archivo: {item['path']}")
        structure = estructura(item)

        LOGGER.debug(f"[PROCESS DATA] - Ejecutando 'origin' - Archivo: {item['path']}")
        origin = create_origin_field(structure)

        LOGGER.debug(f"[PROCESS DATA] - Ejecutando 'platform' - Archivo: {item['path']}")
        platform = create_platform_field(origin)

        LOGGER.debug(f"[PROCESS DATA] - Ejecutando 'hashing' - Archivo: {item['path']}")
        hashing = fingerprint.fingerprint(platform)

        LOGGER.debug(f"[PROCESS DATA] - Completed")

        """Filtro cuando un 'Event number' es 539"""

        if platform["event"]["number"] == "539":

            SessionID = platform.get("Session ID")
            timestamp = platform.get("@timestamp")

            if SessionID in TEMP.keys():
                tiempo = TEMP.get(SessionID)[0]
                new_timestamp = dateutil.parser.isoparse(tiempo)
                timestamp = dateutil.parser.isoparse(timestamp)
                #new_timestamp = datetime.strptime(tiempo, "%Y-%m-%dT%H:%M:%S.%fZ")
                #timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                diff = timestamp - new_timestamp

                # Si dos eventos 539 están en un rango de 5 minutos
                if diff <= timedelta(minutes=5):
                    TEMP[SessionID] = [timestamp]
                    platformCopy = platform.copy()
                    platformCopy["event"]["name"] = "Tot. Recuperacion"
                    platformCopy["event"]["number"] = "152"
                    dict(platformCopy)
                    return {
                        "_index": ES_IDX,
                        "_op_type": "index",
                        "_id": hashing + "152",
                        "_source": json.dumps(platformCopy, default=str),
                        "already": True,
                        "linea_actual": item["linea actual"],
                        "path": item["path"],
                        "total_lineas": item["total_lineas"],
                    }
                    # platform['already']: True
            else:
                TEMP[SessionID] = [timestamp]
                platformCopy = platform.copy()
                platformCopy["event"]["name"] = "Tot. Recuperacion"
                platformCopy["event"]["number"] = "152"
                dict(platformCopy)
                return {
                    "_index": ES_IDX,
                    "_op_type": "index",
                    "_id": hashing + "152",
                    "_source": json.dumps(platformCopy, default=str),
                    "already": True,
                    "linea_actual": item["linea actual"],
                    "path": item["path"],
                    "total_lineas": item["total_lineas"],
                }
        dict(platform)
        return {
            "_index": ES_IDX,
            "_op_type": "index",
            "_id": hashing,
            "_source": json.dumps(platform, default=str),
            "linea_actual": item["linea actual"],
            "path": item["path"],
            "total_lineas": item["total_lineas"],
        }
    except Exception as e:
        LOGGER.error("[FIELD_LIST] - Ha ocurrido un error: %s", e)


def create_origin_field(string):
    """Crea un campo 'Origin' de acuerdo a la ruta del archivo"""

    try:
        LOGGER.debug("[CREATE_ORIGIN_FIELD]")
        LOGGER.debug("[CREATE_ORIGIN_FIELD] - Start...")
        path = string["path"]

        if "fija" in path:
            string["origen"] = "fija"
        elif "movil" in path:
            string["origen"] = "movil"

        LOGGER.debug("[CREATE_ORIGIN_FIELD] - Terminated...")
        return string
    except Exception as e:
        LOGGER.error("[CREATE_ORIGIN_FIELD] - Ha ocurrido un error: %s", e)


def create_platform_field(string):
    """Crea un campo ‘platform’ de acuerdo al ‘path’"""

    try:
        LOGGER.debug("[CREATE_PLATFORM_FIELD]")
        LOGGER.debug("[CREATE_PLATFORM_FIELD] - Start...")
        paths = string.get("path").split("_")
        string["plataforma"] = paths[1].lower()
        string["path"] = "_".join(paths)
        LOGGER.debug("[CREATE_PLATFORM_FIELD] - Terminated...")
        return string

    except Exception as e:
        LOGGER.error("[CREATE_PLATFORM_FIELD] - Ha ocurrido un error: %s", e)
