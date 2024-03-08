import logging
from datetime import date

logger = logging.getLogger(__name__)

class OfcEvent:

    def __init__(self, appl, rep_num,descrip1,descrip2,descrip3,rep_type):
        self.appl = appl
        self.rep_num = rep_num
        self.descrip1 = descrip1
        self.descrip2 = descrip2
        self.descrip3 = descrip3
        self.rep_type = rep_type

class OfcEventDet:

    eventsFile = 'dump_bicssdbdata.txt'
    ofcEvents = []

    def __init__(self, eventsFile,ignoredEvents):
        OfcEventDet.eventsFile = eventsFile
        OfcEventDet.ignoredEvents = ignoredEvents
        OfcEventDet.loadEvents()

    @staticmethod
    def loadEvents():        

        file = open(OfcEventDet.eventsFile, 'r')
        fileLines = file.readlines()

        count = 0
        while len(fileLines) != 0 and count+17<len(fileLines):
            #print(fileLines[count])
            if "APPL" in fileLines[count] and "REP-NUM" in fileLines[count+1] and "DESCRIP1" in fileLines[count+2] and "DESCRIP2" in fileLines[count+3] and "DESCRIP3" in fileLines[count+4] and "REP-TYPE" in fileLines[count+5]:
                ofcEventAux = OfcEvent(fileLines[count][29:70].strip(), fileLines[count+1][44:47].strip(),fileLines[count+2][29:70].strip(),"","",fileLines[count+5][45:47].strip())
                OfcEventDet.ofcEvents.append(ofcEventAux)
                #print(ofcEventAux.appl,ofcEventAux.rep_num,ofcEventAux.descrip1,ofcEventAux.descrip2,ofcEventAux.descrip3,ofcEventAux.rep_type)
                count+=17
            else:
                count+=1   

    @staticmethod
    def check(evNum,evTyp):
        if evNum not in OfcEventDet.ignoredEvents:
            i=0
            while i < len(OfcEventDet.ofcEvents):
                if OfcEventDet.ofcEvents[i].rep_num == evNum and OfcEventDet.ofcEvents[i].rep_type == evTyp:
                    #print('Evento detectado:',evNum,'de tipo',evTyp)
                    #print('Descripción asociada:',OfcEventDet.ofcEvents[i].descrip1)
                    logger.info('Evento %s detectado de tipo %s',evNum,evTyp)
                    logger.info('Descripción asociada: %s',OfcEventDet.ofcEvents[i].descrip1)
                    break
                else:
                    i += 1
        else:
            #print('Evento detectado:',evNum,'de tipo',evTyp,'PERO IGNORADO POR CONFIGURACIÓN')
            logger.info('Evento %s detectado de tipo %s %s',evNum,evTyp,'PERO IGNORADO POR CONFIGURACIÓN')

    @staticmethod
    def getEventName(evNum,evTyp):
        evName = ''
        
        if evNum not in OfcEventDet.ignoredEvents:
            i=0
            while i < len(OfcEventDet.ofcEvents):
                if OfcEventDet.ofcEvents[i].rep_num == evNum and OfcEventDet.ofcEvents[i].rep_type == evTyp:
                    evName = OfcEventDet.ofcEvents[i].descrip1
                    logger.info('Evento %s detectado de tipo %s',evNum,evTyp)
                    break
                else:
                    i += 1
        else:
            #print('Evento detectado:',evNum,'de tipo',evTyp,'PERO IGNORADO POR CONFIGURACIÓN')
            logger.info('Evento %s detectado de tipo %s %s',evNum,evTyp,'PERO IGNORADO POR CONFIGURACIÓN')
        
        return evName

    @staticmethod
    def dumpEvents():
         i=0
         while i < len(OfcEventDet.ofcEvents):
             print(OfcEventDet.ofcEvents[i].rep_num,OfcEventDet.ofcEvents[i].rep_type,OfcEventDet.ofcEvents[i].descrip1,OfcEventDet.ofcEvents[i].descrip2,OfcEventDet.ofcEvents[i].descrip3)
             i += 1