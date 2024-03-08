import configparser
import hashlib
import logging.config
from datetime import date

"""Inicialización y configuración de logging"""

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
# logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)

logging.config.fileConfig("./logger.ini")
LOGGER = logging.getLogger(__name__)
logging.getLogger("elastic_transport.transport").setLevel(logging.INFO)
hoy = date.today()
fh = logging.FileHandler(f'logs/log-file-{hoy}.log')
formatter = logging.Formatter('%(asctime)s :: %(name)-10s :: %(levelname)-10s :: %(message)-10s')
fh.setFormatter(formatter)
LOGGER.addHandler(fh)


def fingerprint(data):
    """Crea un hash basado en el campo ‘message’, usando el algoritmo sha256"""

    try:
        LOGGER.debug("[FINGERPRINT]")
        LOGGER.debug("[FINGERPRINT] - Start...")
        message = data.get("message").encode("utf-8")
        string = hashlib.sha256(message).hexdigest()

        LOGGER.debug("[FINGERPRINT] - Terminated...")
        return string
    except Exception as e:
        LOGGER.error("[FINGERPRINT] - Ha ocurrido un error: %s", e)
