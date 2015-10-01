import model
import util
import os
import mmap
import sys

class SourceProcessor(object):
    __registered = ["SourceFff", "SourceCardfile"]
    __configuredSource = None
        
    def __init__(self, map_context):
        # Configurando el origen de datos
        source_nodes = map_context.xpathEval("/map/source")
        if(source_nodes is not None):
            for clazz in self.__registered:
                constructor = getattr(sys.modules[__name__], clazz) 
                source = constructor(source_nodes[0])
                
                if(source.isConfigured()):
                    self.__configuredSource = source
    
    def getConfiguredProcessor(self):
        if(self.__configuredSource is None):
            raise AssertionError("Source no configurado")
        
        return self.__configuredSource 


class SourceFff(model.RowProcessor):
    ETL_SOURCE_FFF_MODE = 1
    ETL_SOURCE_FFF_XMLNODE = "fff_document"
    ETL_SOURCE_FFF_ATTR_PATH = "path"
    
    source_file_mmap = None
    fields = {}
    line = None
    __last_line = False
    
    def __init__(self, map_node):
        try:
            fff_node = util.getFirstChildNode(map_node, self.ETL_SOURCE_FFF_XMLNODE)
        except LookupError:
            return
        
        file_path = fff_node.prop(self.ETL_SOURCE_FFF_ATTR_PATH)
        
        if(file_path is not None):
            if(file_path.startswith("$")):
                global_path = "-" + file_path[1:]  #slice desde el segundo caracter para quitar el $
                if not global_path in util.globalVars():
                    raise AssertionError("El parametro " + global_path + " es requerido por el atributo " + self.ETL_SOURCE_FFF_ATTR_PATH)
                 
                file_path = util.getGlobalVar(global_path)
                
            source_file = open(file_path, "r+")
            size = os.path.getsize(file_path)
            self.source_file_mmap = mmap.mmap(source_file.fileno(), size)

    def mode(self):
        return self.ETL_SOURCE_FFF_MODE
    
    def isConfigured(self):
        return self.source_file_mmap is not None
    
    def confField(self, field_id, map_node):
        field_node = util.getFirstChildNode(map_node, self.ETL_FIELD_SOURCE_XMLNODE)
        field = FieldSourceFff(field_node)
        self.fields[field_id] = field
        
    def nextRow(self):
        if(not self.__last_line):
            self.line = self.source_file_mmap.readline()
            self.line = util.cleanLine(self.line)
            self.__last_line = (self.source_file_mmap.tell() + 2) > self.source_file_mmap.size()
            return True
        else:
            return False
        
    def processField(self, field_id, data):
        field = self.fields[field_id]
        value = field.processValue(self.line)[0]
        is_valid = field.validate(value)
        
        return field.postProcessValue(value)
    
    
class FieldSourceFff(model.FieldProcessor):
    ETL_FIELD_SOURCE_FFF_XMLNODE = "fixed"
    ETL_FIELD_SOURCE_FFF_ATTR_FROM = "from"
    ETL_FIELD_SOURCE_FFF_ATTR_TO = "to"
    
    def __init__(self, map_node):
        fixed_node = util.getFirstChildNode(map_node, self.ETL_FIELD_SOURCE_FFF_XMLNODE)
        try:
            from_prop = int(fixed_node.prop(self.ETL_FIELD_SOURCE_FFF_ATTR_FROM))
            
            if(fixed_node.prop(self.ETL_FIELD_SOURCE_FFF_ATTR_TO) is not None):
                to_prop = int(fixed_node.prop(self.ETL_FIELD_SOURCE_FFF_ATTR_TO))
            else:
                to_prop = None
                
        except ValueError:
            raise ValueError("Los atributos from y to deben ser numeros enteros")
        
        self.attrs = [from_prop, to_prop]

        super(FieldSourceFff, self).__init__(map_node)

    def processValue(self, data):
        from_prop = self.attrs[0]
        to_prop = self.attrs[1]
        value = data[from_prop:to_prop]
         
        return [value, True]


class SourceCardfile(model.RowProcessor):
    ETL_SOURCE_CRDF_MODE = 2
    ETL_SOURCE_CRDF_XMLNODE = "cardfile_document"
    ETL_SOURCE_CRDF_ATTR_PATH = "path"
    
    source_file_mmap = None
    fields = {}
    line = None
    line_counter = 0
    
    def __init__(self, map_node):
        try:
            crdf_node = util.getFirstChildNode(map_node, self.ETL_SOURCE_CRDF_XMLNODE)
        except LookupError:
            return
        
        file_path = crdf_node.prop(self.ETL_SOURCE_CRDF_ATTR_PATH)
        
        if(file_path is not None):
            if(file_path.startswith("$")):
                global_path = "-" + file_path[1:]  #slice desde el segundo caracter para quitar el $
                if not global_path in util.globalVars():
                    raise AssertionError("El parametro " + global_path + " es requerido por el atributo " + self.ETL_SOURCE_CRDF_ATTR_PATH)
                 
                file_path = util.getGlobalVar(global_path)
                
            source_file = open(file_path, "r+")
            size = os.path.getsize(file_path)
            self.source_file_mmap = mmap.mmap(source_file.fileno(), size)

    def mode(self):
        return self.ETL_SOURCE_CRDF_MODE
    
    def isConfigured(self):
        return self.source_file_mmap is not None
    
    def confField(self, field_id, map_node):
        field_node = util.getFirstChildNode(map_node, self.ETL_FIELD_SOURCE_XMLNODE)
        field = FieldSourceCardfile(field_node)
        self.fields[field_id] = field
        
    def nextRow(self):
        self.line = self.source_file_mmap.readline()
        self.line = util.cleanLine(self.line)
        self.line_counter = self.line_counter + 1
        
        return len(self.line) > 0
        
    def processField(self, field_id, data):
        field = self.fields[field_id]
        value = field.processValue([self.line, self.line_counter])
        
        if(value[1] is True):
            is_valid = field.validate(value[0])
            return field.postProcessValue(value[0])
        else:
            return field.postProcessValue(value[0])
    
    
class FieldSourceCardfile(model.FieldProcessor):
    ETL_FIELD_SOURCE_CRDF_XMLNODE = "floating_field"
    ETL_FIELD_SOURCE_CRDF_ATTR_LNFRM = "line_from"
    ETL_FIELD_SOURCE_CRDF_ATTR_LNTO = "line_to"
    ETL_FIELD_SOURCE_CRDF_ATTR_COLFRM = "col_from"
    ETL_FIELD_SOURCE_CRDF_ATTR_COLTO = "col_to"
    ETL_FIELD_SOURCE_CRDF_ATTR_TYPE = "type"
    ETL_FIELD_SOURCE_CRDF_ATTR_TYPE_HDR = "header"
    ETL_FIELD_SOURCE_CRDF_ATTR_TYPE_ROW = "row"
    
    header_value = None
    
    def __init__(self, map_node):
        floating_node = util.getFirstChildNode(map_node, self.ETL_FIELD_SOURCE_CRDF_XMLNODE)
        try:
            line_from_prop      = int(floating_node.prop(self.ETL_FIELD_SOURCE_CRDF_ATTR_LNFRM))
            line_to_prop        = int(floating_node.prop(self.ETL_FIELD_SOURCE_CRDF_ATTR_LNTO))
            col_from_prop       = int(floating_node.prop(self.ETL_FIELD_SOURCE_CRDF_ATTR_COLFRM))
            col_to_prop         = int(floating_node.prop(self.ETL_FIELD_SOURCE_CRDF_ATTR_COLTO))
        except ValueError:
            raise ValueError("Los atributos line_from, line_to, col_from y col_to deben ser numeros enteros")
        
        if(floating_node.prop(self.ETL_FIELD_SOURCE_CRDF_ATTR_TYPE) is not None):
            type = floating_node.prop(self.ETL_FIELD_SOURCE_CRDF_ATTR_TYPE)
        else:
            type = self.ETL_FIELD_SOURCE_CRDF_ATTR_TYPE_ROW
        
        self.attrs = [line_from_prop, line_to_prop, col_from_prop, col_to_prop, type]

        super(FieldSourceCardfile, self).__init__(map_node)

    def processValue(self, data):
        type = self.attrs[4]
        
        if(type == self.ETL_FIELD_SOURCE_CRDF_ATTR_TYPE_HDR):
            if(self.header_value is not None):
                return [self.header_value, True]
        
        line_from_prop = self.attrs[0]
        line_to_prop = self.attrs[1]
        col_from_prop = self.attrs[2]
        col_to_prop = self.attrs[3]
        
        line = data[0]
        line_counter = data[1]
        
        if((line_counter >= line_from_prop or line_from_prop == -1) and (line_counter <= line_to_prop or line_to_prop == -1)):
            value = line[col_from_prop:col_to_prop]
            self.header_value = value
            return [value, True]
        else:
            return [None, False]
        

