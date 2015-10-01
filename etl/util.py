import pprint
import __builtin__

CHR_CR = str(chr(13))
CHR_NL = str(chr(10))
__builtin__.globalvars = {}

def getFirstChildNode(parent_node, child_xpath):
    context = parent_node.doc.xpathNewContext()
    context.setContextNode(parent_node)
    childs = context.xpathEval(child_xpath)

    if(len(childs) > 0):
        return childs[0]
    else:
        raise LookupError("No existe el elemento <" + child_xpath + "> en " + parent_node.nodePath())

def cleanLine(line):
    return line.replace(CHR_CR, "").replace(CHR_NL, "")

def getGlobalVar(name):
    return __builtin__.globalvars[name]

def setGlobalVar(name, value):
    __builtin__.globalvars[name] = value
    
def globalVars():
    return __builtin__.globalvars

def dump(obj):
  '''return a printable representation of an object for debugging'''
  newobj=obj
  if '__dict__' in dir(obj):
    newobj=obj.__dict__
    if ' object at ' in str(obj) and not newobj.has_key('__type__'):
      newobj['__type__']=str(obj)
    for attr in newobj:
      newobj[attr]=dump(newobj[attr])
      
  pprint.pprint(newobj)


