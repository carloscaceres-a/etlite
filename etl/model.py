import re
import util

# Definiciones base
class RowProcessor(object):
    ETL_FIELD_TARGET_XMLNODE = "target"
    ETL_FIELD_SOURCE_XMLNODE = "source"
    ETL_FIELD_TRANSFORMATIONS_XMLNODE = "transformations"
    
    def __init__(self, map_node):
        pass
    
    def mode(self):
        pass
    
    def isConfigured(self):
        pass
    
    def confField(self, id, map_node):
        pass

    def nextRow(self):
        return True
        
    def dropRow(self):
        pass
    
    def processField(self, field_id, data):
        pass
    
    def flush(self):
        pass


class FieldProcessor(object):
    ETL_FIELD_EXPECTS_XMLNODE = "expects"
    ETL_FIELD_MODE_XMLNODE = "mode"
    ETL_FIELD_REQUIRED_XMLNODE = "required"
#    ETL_FIELD_VARIABLE_XMLNODE = "variable"
    
    attrs = None
    __expects = None
    __required = True
    __mode = None
    __holded_value = None
#    __variable_name = None
#    __variable_value = None
    
    def __init__(self, map_node):
        # Validacion por formato
        expects_node = None
        try:
            expects_node = util.getFirstChildNode(map_node, self.ETL_FIELD_EXPECTS_XMLNODE)
        except LookupError:
            pass
        
        if(expects_node is not None):
            regex = expects_node.getContent()
            self.__expects = re.compile(regex)

        # Es valido si no se encuentra
        required_node = None
        try:
            required_node = util.getFirstChildNode(map_node, self.ETL_FIELD_REQUIRED_XMLNODE)
        except LookupError:
            pass
        
        if(required_node is not None):
            self.__required = False if required_node.getContent() == "no" else self.__required

        # Tratamiento especial
        mode_node = None
        try:
            mode_node = util.getFirstChildNode(map_node, self.ETL_FIELD_MODE_XMLNODE)
        except LookupError:
            pass
        
        if(mode_node is not None):
            self.__mode = mode_node.getContent()

#        variable_node = None
#        
#        try:
#            variable_node = util.getFirstChildNode(map_node, self.ETL_FIELD_VARIABLE_XMLNODE)
#        except LookupError:
#            pass
#        
#        if(variable_node is not None):
#            self.__variable_name = variable_node.getContent()

    def isConfigured(self):
        pass
    
    def validate(self, value):
        if(self.__expects is not None):
            match = self.__expects.match(value)
            return False if match is None else True
        else:
            return True
        
    def processValue(self, data):
#        if(self.__variable_name is not None):
#            return [self.__variable_value, False]
        return self.postProcessValue(data)
        
    def postProcessValue(self, data):
        is_valid = self.validate(data)

        if is_valid:
            self.__holded_value = data

        if not self.__required and not is_valid:
            is_valid = True
            data = None

        if self.__mode == "hold" and not self.__required:
            return [self.__holded_value, True]
        else:
            return [data, is_valid]
    
    
    
    
    
    
    
    
    