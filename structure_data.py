import configparser
import datetime
import logging.config
import platform
from collections import defaultdict
from datetime import date
from dateutil import tz
from dateutil import parser
import pytz

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
# logging.getLogger("elastic_transport.transport").setLevel(logging.INFO)


logging.config.fileConfig("./logger.ini")
logging.getLogger("elastic_transport.transport").setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)
hoy = date.today()
fh = logging.FileHandler(f'logs/log-file-{hoy}.log')
formatter = logging.Formatter('%(asctime)-10s :: %(name)-10s :: %(levelname)-10s :: %(message)30s')
fh.setFormatter(formatter)
LOGGER.addHandler(fh)


def estructura(dato):
    """Crea una estructura de tipo JSON, asignando y organizando cada porción de dígitos a un nombre en específico."""

    LOGGER.debug("[STRUCTURE_DATA]")
    LOGGER.debug("[STRUCTURE_DATA] - Start...")
    try:

        def dictt():
            return defaultdict(dictt)

        lista_final = dictt()  # Se crea 'lista_final' de tipo 'defaultdict'
        string = dato["linea"]
        lista_final["path"] = dato["path"]
        lista_final["message"] = string

        LOGGER.debug("[STRUCTURE_DATA] - Estructurando...")

        """Estructuración inicial"""
        event_number = string[16:19].strip()
        event_type = string[14:16].strip()
        call_duration = 0

        lista_final["Event report date"] = field_list(0, 5, string)

        lista_final["event"]["type"] = event_type
        lista_final["event"]["number"] = event_number
        lista_final["EventTime"]["hour"] = field_list(5, 7, string)
        lista_final["EventTime"]["minutes"] = field_list(7, 9, string)
        lista_final["EventTime"]["seconds"] = field_list(9, 11, string)
        lista_final["EventTime"]["mili"] = field_list(11, 13, string)
        lista_final["Interface level"] = field_list(13, 15, string)
        lista_final["Severity level"] = field_list(19, 21, string)
        lista_final["Application"] = field_list(21, 31, string)
        lista_final["Partition"] = field_list(31, 34, string)
        lista_final["Domain"] = field_list(34, 37, string)
        lista_final["host"] = platform.node()

        if event_number == "152":
            lista_final["event"]["name"] = "Tot. Recuperación"
        elif event_number == "190":
            lista_final["event"]["name"] = "Mensaje nuevo guardado"
        elif event_number == "191":
            lista_final["event"]["name"] = "Mensaje antiguo guardado"
        elif event_number == "192":
            lista_final["event"]["name"] = "Mensaje borrado guardado"
        elif event_number == "194":
            lista_final["event"]["name"] = "Mensaje nuevo borrado"
        elif event_number == "195":
            lista_final["event"]["name"] = "Mensaje antiguo borrado"
        elif event_number == "281":
            lista_final["event"]["name"] = "Llamada completada"
        elif event_number == "290":
            lista_final["event"]["name"] = "Llamada sin mensaje"
        elif event_number == "161":
            lista_final["event"]["name"] = "Mensajes depositados"
        elif event_number == "159":
            lista_final["event"]["name"] = "Devolución completada"
        elif event_number == "539":
            lista_final["event"]["name"] = "Tot. Recuperación"
        elif event_number == "947":
            lista_final["event"]["name"] = "Notificacion de SMPP"
        elif event_number == "006":
            lista_final["event"]["name"] = "Entrega SMPP"
        elif event_number == "666":
            lista_final["event"]["name"] = "Tot. Recuperación Remota"
        elif event_number == "250":
            lista_final["event"]["name"] = "Tot. Alta de buzón"
        elif event_number == "252":
            lista_final["event"]["name"] = "Tot. Baja de buzón"
        elif event_number == "182":
            lista_final["event"]["name"] = "Tot. Buzón lleno"

            ####### EVENT TYPE ########
        LOGGER.debug("[STRUCTURE_DATA] - Definiendo tipo...")

        if event_type == "01":
            event_type_1(event_number, lista_final)

        elif event_type == "02":
            event_type_2(event_number, lista_final, string)

        elif event_type == "03":
            event_type_3(event_number, lista_final, string)

        elif event_type == "04":
            event_type_4(event_number, lista_final, string)

        LOGGER.debug("[STRUCTURE_DATA] - Definiendo campos adicionales...")

        if "Call duration" in lista_final and lista_final["Call duration"] != "":
            lista_final["Call duration"] = float(
                lista_final["Call duration"]) / 100

        varValue = (
                "11/03/2020 "
                + lista_final["EventTime"]["hour"]
                + ":"
                + lista_final["EventTime"]["minutes"]
                + ":"
                + lista_final["EventTime"]["seconds"]
                + "."
                + "00"
        )  # Formato de fechas
           # menos -3600 en invierno y -7200 en verano porque el timestamp es UTC

        difference = (
                             (int(lista_final["Event report date"]) - 23080) * 86400) # - 7200

        lista_final["@timestamp"] = datetime.datetime.strptime(
            varValue, "%d/%m/%Y %H:%M:%S.%f"
        ) + datetime.timedelta(seconds=difference)
        madrid = pytz.timezone('Europe/Madrid')
        fecha = madrid.localize(lista_final["@timestamp"])
        datetime.datetime.strftime(fecha, "%d/%m/%Y %H:%M:%S%z")
        fecha_3 = datetime.datetime.strptime(datetime.datetime.strftime(fecha, "%d/%m/%Y %H:%M:%S%z"),"%d/%m/%Y %H:%M:%S%z")
        lista_final["@timestamp"] = fecha_3
        lista_final["call"] = {"gte": 0, "lte": 0}
        lista_final["call"]["gte"] = lista_final["@timestamp"]

        if "Call duration" in lista_final and lista_final["Call duration"] != "":
            lista_final["call"]["lte"] = lista_final["@timestamp"] + datetime.timedelta(
                seconds=float(lista_final["Call duration"])
            )
        else:
            lista_final["call"]["lte"] = lista_final["@timestamp"]

        lista_final = {k: v for k, v in lista_final.items() if v}

        LOGGER.debug("[STRUCTURE_DATA] - Terminated...")

        lista_final["@timestamp"] = datetime.datetime.isoformat(lista_final["@timestamp"])
        lista_final["call"]["gte"] = datetime.datetime.isoformat(lista_final["call"]["gte"])
        lista_final["call"]["lte"] = datetime.datetime.isoformat(lista_final["call"]["lte"])


        return lista_final

    except Exception as e:
        LOGGER.error("[STRUCTURE_DATA] - Ha ocurrido un error: %s", e)


def field_list(rangoInicial, rangoFinal, string):
    """
    Se define un campo en el diccionario

    Args:
        rangoInicial (type int)
        rangoFinal (type int)
        string (typo string) - linea de datos obtenido por read_file()
    """
    try:
        return string[rangoInicial:rangoFinal].strip()
    except Exception as e:
        LOGGER.error(
            "[FIELD_LIST] - Ha ocurrido un error: %s", e, " - Doc: ", string["message"]
        )


def event_type_1(event_number, lista_final):
    """Función que define unos campos cuando ‘event_type’ es 1"""
    try:
        if event_number == "004" or event_number == "010":
            lista_final["event"]["name"] = "MWI"
            lista_final["event"]["call"]["type"] = "MWI"

        return lista_final
    except Exception as e:
        LOGGER.error("[EVENT_TYPE_1] - Ha ocurrido un error: %s", e)


def event_type_2(event_number, lista_final, string):
    """Función que define unos campos cuando ‘event_type’ es 2"""
    try:
        lista_final["Customer ID"] = field_list(37, 51, string)
        lista_final["Mailbox Country Code"] = field_list(51, 55, string)
        lista_final["Mailbox ID Extension"] = field_list(55, 57, string)
        lista_final["Mailbox Guest"] = field_list(57, 59, string)
        lista_final["Session ID"] = field_list(61, 73, string)
        lista_final["Log flag"] = field_list(73, 75, string)
        lista_final["Node ID"] = field_list(74, 79, string)
        lista_final["Partition number"] = field_list(79, 82, string)
        lista_final["Domain number"] = field_list(82, 85, string)
        lista_final["Bridge/DID Mailbox Number"] = field_list(85, 99, string)
        lista_final["Bridge/DID Country Code"] = field_list(99, 103, string)
        lista_final["Bridge/DID Extension"] = field_list(103, 105, string)
        lista_final["Billing Number"] = field_list(105, 125, string)
        lista_final["Billing Group"] = field_list(125, 130, string)
        lista_final["Class of Service"] = field_list(130, 135, string)
        lista_final["Mailbox Type"] = field_list(135, 137, string)
        lista_final["Local Area"] = field_list(137, 141, string)
        lista_final["Internal Count 1"] = field_list(141, 147, string)
        lista_final["Customized Count 46"] = field_list(147, 149, string)
        lista_final["Customized Count 47"] = field_list(148, 150, string)
        lista_final["Customized Count 48"] = field_list(149, 151, string)
        lista_final["Customized Count 49"] = field_list(150, 152, string)
        lista_final["Customized Count 50"] = field_list(151, 153, string)
        lista_final["Customized Count 51"] = field_list(152, 154, string)
        lista_final["Customized Count 1"] = field_list(153, 156, string)
        lista_final["Customized Count 2"] = field_list(156, 159, string)
        lista_final["Customized Count 3"] = field_list(159, 162, string)
        lista_final["Customized Count 4"] = field_list(162, 165, string)
        lista_final["Customized Count 5"] = field_list(165, 168, string)
        lista_final["Customized Count 6"] = field_list(168, 170, string)
        lista_final["Customized Count 7"] = field_list(170, 172, string)
        lista_final["Customized Count 8"] = field_list(172, 174, string)
        lista_final["Customized Count 9"] = field_list(174, 176, string)
        lista_final["Customized Count 10"] = field_list(176, 178, string)
        lista_final["Customized Count 11"] = field_list(178, 180, string)
        lista_final["Customized Count 12"] = field_list(179, 181, string)
        lista_final["Customized Count 13"] = field_list(180, 182, string)
        lista_final["Customized Count 14"] = field_list(181, 183, string)
        lista_final["Customized Count 15"] = field_list(182, 184, string)
        lista_final["Customized Count 16"] = field_list(183, 185, string)
        lista_final["Customized Count 17"] = field_list(184, 186, string)
        lista_final["Customized Count 18"] = field_list(185, 187, string)
        lista_final["Customized Count 19"] = field_list(186, 188, string)
        lista_final["Customized Count 20"] = field_list(187, 189, string)
        lista_final["Customized Count 21"] = field_list(188, 190, string)
        lista_final["Customized Count 22"] = field_list(189, 191, string)
        lista_final["Customized Count 23"] = field_list(190, 192, string)
        lista_final["Customized Count 24"] = field_list(191, 193, string)
        lista_final["Customized Count 25"] = field_list(192, 194, string)
        lista_final["Customized Count 26"] = field_list(193, 195, string)
        lista_final["Customized Count 27"] = field_list(194, 196, string)
        lista_final["Customized Count 28"] = field_list(195, 197, string)
        lista_final["Customized Count 29"] = field_list(196, 198, string)
        lista_final["Customized Count 30"] = field_list(197, 199, string)
        lista_final["Customized Count 31"] = field_list(198, 200, string)
        lista_final["Customized Count 32"] = field_list(199, 201, string)
        lista_final["Customized Count 33"] = field_list(200, 202, string)
        lista_final["Customized Count 34"] = field_list(201, 203, string)
        lista_final["Customized Count 35"] = field_list(202, 204, string)
        lista_final["Customized Count 36"] = field_list(203, 205, string)
        lista_final["Customized Count 37"] = field_list(204, 206, string)
        lista_final["Customized Count 38"] = field_list(205, 207, string)
        lista_final["Customized Count 39"] = field_list(206, 208, string)
        lista_final["Customized Count 40"] = field_list(207, 209, string)
        lista_final["Customized Count 41"] = field_list(208, 210, string)
        lista_final["Customized Count 42"] = field_list(209, 211, string)
        lista_final["Customized Count 43"] = field_list(210, 212, string)
        lista_final["Customized Count 44"] = field_list(211, 213, string)
        lista_final["Customized Count 45"] = field_list(212, 214, string)
        lista_final["Call start date"] = field_list(213, 218, string)
        lista_final["Call start time"] = field_list(218, 226, string)
        lista_final["Call duration"] = field_list(226, 233, string)
        lista_final["Called number"] = field_list(233, 257, string)
        lista_final["Calling number"] = field_list(257, 277, string)
        lista_final["NAP call type"] = field_list(277, 283, string)
        lista_final["NAP port type"] = field_list(283, 285, string)
        lista_final["NAP port selector"] = field_list(284, 287, string)
        lista_final["NAP port number"] = field_list(287, 293, string)
        lista_final["NAP pulse rule"] = field_list(293, 295, string)
        lista_final["CASCADA"] = field_list(295, 301, string)
        lista_final["Numero al que notificar por SM"] = field_list(
            301, 321, string)
        lista_final["RDSI"] = field_list(321, 323, string)
        lista_final["Notificacion por MWI"] = field_list(322, 324, string)
        lista_final["Extension Allowed"] = field_list(323, 325, string)
        lista_final["Numero Maximo de Extensiones"] = field_list(
            324, 326, string)

        lista_final["Recuperación"] = field_list(326, 329, string)
        lista_final["Numero mensajes viejos en recuperacion"] = field_list(
            329, 332, string
        )
        lista_final["Numero de llamadas de devolucion"] = field_list(
            332, 334, string)
        lista_final["Identificacion de llamante activado"] = field_list(
            334, 336, string
        )
        lista_final["Devolución automatica o manual"] = field_list(
            335, 337, string)
        lista_final["Devolución por Depósito/Llamada perdida"] = field_list(
            336, 338, string
        )

        if event_number == "152" or event_number == "666":
            if (
                    lista_final["Customized Count 8"] >= "01"
                    and lista_final["Customized Count 40"] == "01"
                    or lista_final["Customized Count 22"] == "01"
                    or lista_final["Customized Count 41"] == "01"
            ):
                lista_final["event"]["call"]["type"] = [
                    "Esc. Mensajes Nuevos",
                    "Configuración",
                ]
            elif lista_final["Customized Count 8"] >= "01":
                lista_final["event"]["call"]["type"] = "Esc. Mensajes Nuevos"
            elif (
                    lista_final["Customized Count 38"] == "1"
                    or lista_final["Customized Count 40"] == "1"
                    or lista_final["Customized Count 22"] == "1"
                    or lista_final["Customized Count 41"] == "1"
            ):
                lista_final["event"]["call"]["type"] = "Configuración"

        if event_number == "151":
            if (lista_final["Customized Count 2"] == "000" or lista_final["Customized Count 2"] is None or lista_final[
                "Customized Count 2"] == "" or lista_final["Customized Count 2"] == "   "):
                lista_final["event"]["name"] = "Tot. Depósito"
                if lista_final["Customized Count 1"] == "001":
                    lista_final["event"]["call"]["type"] = "Deja Mensaje"
                else:
                    lista_final["event"]["call"]["type"] = "Sin Mensaje"

        if event_number == "281":
            lista_final["event"]["name"] = "Llamada de voz"
            lista_final["event"]["call"]["type"] = "Voz"

        if event_number == "539":
            lista_final["event"]["call"]["type"] = "Devolución"

        if event_number == "852":
            lista_final["event"]["name"] = "Tot. Recuperación de video"
            lista_final["event"]["call"][
                "type"
            ] = "Llamada de recuperación directa de video"

        if event_number == "851":
            lista_final["event"]["name"] = "Tot. Depósito de video"
            lista_final["event"]["call"]["type"] = "Llamada de depósito de video"

        return lista_final
    except Exception as e:
        LOGGER.error("[FIELD_LIST] - Ha ocurrido un error: %s", e)


def event_type_3(event_number, lista_final, string):
    """Función que define unos campos cuando ‘event_type’ es 3"""
    try:
        lista_final["Customer ID"] = field_list(37, 51, string)
        lista_final["Mailbox Country Code"] = field_list(51, 55, string)
        lista_final["Mailbox ID Extension"] = field_list(55, 57, string)
        lista_final["Mailbox Guest"] = field_list(57, 59, string)
        lista_final["Session ID"] = field_list(61, 73, string)
        lista_final["Log flag"] = field_list(73, 75, string)
        lista_final["Node ID"] = field_list(74, 79, string)
        lista_final["Partition number"] = field_list(79, 82, string)
        lista_final["Domain number"] = field_list(82, 85, string)
        lista_final["Bridge/DID Mailbox Number"] = field_list(85, 99, string)
        lista_final["Bridge/DID Country Code"] = field_list(99, 103, string)
        lista_final["Bridge/DID Extension"] = field_list(103, 105, string)
        lista_final["Billing Number"] = field_list(105, 125, string)
        lista_final["Billing Group"] = field_list(125, 130, string)
        lista_final["Class of Service"] = field_list(130, 135, string)
        lista_final["Mailbox Type"] = field_list(135, 137, string)
        lista_final["Local Area"] = field_list(137, 141, string)
        lista_final["Other customer id"] = field_list(141, 155, string)
        lista_final["Other customer id country code"] = field_list(
            155, 159, string)
        lista_final["Other customer id extension number"] = field_list(
            159, 161, string)
        lista_final["Other customer id guest number"] = field_list(
            161, 163, string)
        lista_final["Send date"] = field_list(165, 170, string)
        lista_final["Send time"] = field_list(170, 178, string)
        lista_final["Transaction number"] = field_list(178, 190, string)
        lista_final["Sub-transaction number"] = field_list(190, 196, string)
        lista_final["Queue date"] = field_list(196, 201, string)
        lista_final["Queue time"] = field_list(201, 209, string)
        lista_final["Last recipient indicator"] = field_list(209, 211, string)
        lista_final["Session ID"] = field_list(210, 222, string)
        lista_final["Message duration"] = field_list(222, 228, string)
        lista_final["Compression ratio"] = field_list(228, 230, string)
        lista_final["Privacy"] = field_list(229, 231, string)
        lista_final["Group number"] = field_list(230, 233, string)
        lista_final["Notification"] = field_list(233, 235, string)
        lista_final["Return-to-sender reason"] = field_list(234, 236, string)
        lista_final["Phone number"] = field_list(236, 256, string)
        lista_final["Original transaction number"] = field_list(
            256, 268, string)
        lista_final["Original sub-transaction number"] = field_list(
            268, 274, string)
        lista_final["Function (Reply/Copy)"] = field_list(274, 276, string)
        lista_final["Fax Duration"] = field_list(275, 280, string)
        lista_final["Fax Storage (Kilo Bytes)"] = field_list(280, 286, string)
        lista_final["Fax Pages"] = field_list(286, 290, string)
        lista_final["Carrier"] = field_list(290, 294, string)
        lista_final["Networking required"] = field_list(294, 296, string)
        lista_final["Recipient outside senders area code"] = field_list(
            295, 297, string
        )
        lista_final["Recipient not local"] = field_list(296, 298, string)
        lista_final["Recipient outside senders country"] = field_list(
            297, 299, string)
        lista_final["Escuchada Numero Opcion del Menu de Info"] = field_list(
            298, 300, string
        )
        lista_final["Recip. in different domain type"] = field_list(
            298, 300, string)
        lista_final["Recipient is foreign mbx (casual)"] = field_list(
            299, 301, string)
        lista_final["System generated message"] = field_list(300, 302, string)
        lista_final["Interrumpid Locucion Menu de Informacion"] = field_list(
            302, 304, string
        )
        lista_final["Error ind."] = field_list(309, 311, string)
        lista_final["Mailbox type"] = field_list(311, 313, string)
        lista_final["Node ID"] = field_list(313, 318, string)
        lista_final["System number"] = field_list(318, 336, string)
        lista_final["Destination partition ID"] = field_list(336, 339, string)
        lista_final["Destination partition number"] = field_list(
            339, 342, string)
        lista_final["Destination domain ID"] = field_list(342, 345, string)
        lista_final["Destination domain number"] = field_list(345, 348, string)
        lista_final["Local Area"] = field_list(348, 352, string)
        lista_final["RDSI"] = field_list(352, 354, string)

        if (
                event_number == "500"
                or event_number == "501"
                or event_number == "502"
                or event_number == "503"
        ):
            lista_final["event"]["name"] = "Tot. Depósito de fax"

            if event_number == "500" or event_number == "501":
                lista_final["event"]["call"]["type"] = "Deposita fax"

            if event_number == "502" or event_number == "503":
                lista_final["event"]["call"]["type"] = "No deposita fax"

        if event_number == "553" or event_number == "530":
            lista_final["event"]["call"]["type"] = "Fax desde web BU"

        if event_number == "520":
            lista_final["event"]["call"]["type"] = "Impresión desde TUI"

        if event_number == "163" or event_number == "165" or event_number == "236":
            lista_final["event"]["name"] = "Mensavoz"

        return lista_final
    except Exception as e:
        LOGGER.error("[FIELD_LIST] - Ha ocurrido un error: %s", e)


def event_type_4(event_number, lista_final, string):
    """Función que define unos campos cuando ‘event_type’ es 4"""
    try:
        lista_final["Mailbox Number"] = field_list(37, 51, string)
        lista_final["Country Code"] = field_list(51, 55, string)
        lista_final["Extension"] = field_list(55, 57, string)
        lista_final["Guest"] = field_list(57, 59, string)
        lista_final["Session ID"] = field_list(61, 73, string)
        lista_final["Log flag"] = field_list(73, 75, string)
        lista_final["Node ID"] = field_list(74, 79, string)
        lista_final["Partition number"] = field_list(79, 82, string)
        lista_final["Domain number"] = field_list(82, 85, string)
        lista_final["Bridge/DID Mailbox Number"] = field_list(85, 99, string)
        lista_final["Bridge/DID Country Code"] = field_list(99, 103, string)
        lista_final["Bridge/DID Extension"] = field_list(103, 105, string)
        lista_final["Billing Number"] = field_list(105, 125, string)
        lista_final["Billing Group"] = field_list(125, 130, string)
        lista_final["Class of Service"] = field_list(130, 135, string)
        lista_final["Mailbox Type"] = field_list(135, 137, string)
        lista_final["Local Area"] = field_list(137, 141, string)
        lista_final["MPS result"] = field_list(141, 144, string)
        lista_final["New Value"] = field_list(141, 197, string)
        lista_final["Old Value"] = field_list(197, 253, string)
        lista_final["Admin User"] = field_list(253, 263, string)
        lista_final["Admin Station"] = field_list(263, 280, string)
        lista_final["Admin Module"] = field_list(280, 290, string)
        lista_final["MPS Result"] = field_list(290, 293, string)
        lista_final["Subscriber GUID"] = field_list(293, 325, string)

        if event_number == "251":
            lista_final["event"]["call"]["type"] = "Configuración"

        if lista_final.get("Old Value")[0:12] == "Submit EMAIL":
            lista_final["event"]["name"] = "Mail"
            lista_final["event"]["call"]["type"] = "Mail"

        elif lista_final.get("Old Value")[0:10] == "Submit SMS":
            lista_final["event"]["call"]["type"] = "SMS"
        else:
            lista_final["Old Value"] = lista_final.get("Old Value")

        return lista_final
    except Exception as e:
        LOGGER.error("[FIELD_LIST] - Ha ocurrido un error: %s", e)
