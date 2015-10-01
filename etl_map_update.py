import libxml2
import os
import sys
import pprint
import mmap
from etl import controller, util
from sre_compile import isstring
import datetime

print "Iniciando etl_map_update... " + str(datetime.datetime.now())

# Obtencion de parametros
if len(sys.argv) < 2:
    print "Uso: python etl_map_update -map /ruta/a/map_file.xml [-input_file /ruta/al/archivo/de/entrada]"
    sys.exit(1)
else:
    for n in range(1, len(sys.argv), 2):
        if sys.argv[n].startswith("-") and (n + 1) < len(sys.argv):
            util.setGlobalVar(sys.argv[n], sys.argv[n + 1])

param_map = None
if "-map" in util.globalVars():
    param_map = util.getGlobalVar("-map")
else:
    print "El parametro -map es requerido"
    sys.exit(1)

if not os.path.exists(param_map):
    print "El archivo " + param_map + " no existe"
    sys.exit(1)
    
try: 
    map = libxml2.parseFile(param_map)
except:
    print "El archivo " + param_map + " no es un XML valido"
    sys.exit(1)


controller.processMapFile(map)

print "Finalizando etl_map_update... " + str(datetime.datetime.now())