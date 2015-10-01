import model
import util
import string
import MySQLdb #Se requiere instalar
import datetime

class TransformationProcessor(object):
    __configuredTransformation = None
        
    def __init__(self, map_context):
        map_nodes = map_context.xpathEval("/map/transformation")
        self.__configuredTransformation = Transformation(map_nodes)
    
    def getConfiguredProcessor(self):
        return self.__configuredTransformation
    


class Transformation(model.RowProcessor):
    __registered = ["Trim", "Dictionary", "SaveTo", "JoinWith", "Wrap", "DateSufix"]
    fields = {}
        
    def __init__(self, map_node):
        pass

    def isConfigured(self):
        return True
    
    def confField(self, field_id, map_node): 
        transformation_node = None
        
        try:
            transformation_node = util.getFirstChildNode(map_node, self.ETL_FIELD_TRANSFORMATIONS_XMLNODE)
        except LookupError:
            pass
        
        if(transformation_node is not None):
            for clazz in self.__registered:
                transformation = globals()[clazz](transformation_node)
                
                if(not self.fields.has_key(field_id)):
                    self.fields[field_id] = []
                
                if(transformation.isConfigured()):
                    self.fields[field_id].append(transformation)

        
    def processField(self, field_id, data):
        result = [data, False]
        
        if(self.fields.has_key(field_id)):
            for t in self.fields[field_id]:
                transform = t.processValue(result[0])
                result[0] = transform[0]
                result[1] = result[1] or transform[1]
                
        return result



class Dictionary(model.FieldProcessor):
    ETL_DICTIONARY_XMLNODE = "dictionary"
    ETL_DICTIONARY_DB_XMLNODE = "database"
    ETL_DICTIONARY_DB_HOST_XMLNODE = "host"
    ETL_DICTIONARY_DB_USER_XMLNODE = "user"
    ETL_DICTIONARY_DB_PASS_XMLNODE = "pass"
    ETL_DICTIONARY_DB_DBNAME_XMLNODE = "dbname"
    ETL_DICTIONARY_DB_QUERY_XMLNODE = "sql_query"
    ETL_DICTIONARY_REPLACE_MODE_ALL = "all"
    ETL_DICTIONARY_REPLACE_MODE_INLINE = "inline"
    ETL_DICTIONARY_IF_MODE_EQUALS = "equals"
    ETL_DICTIONARY_IF_MODE_CONTAINS = "contains"
    
    __replace_mode = None
    __if_mode = None
    __dictionary = None
    
    def __init__(self, map_node):
        dictionary_node = None
        try:
            dictionary_node = util.getFirstChildNode(map_node, self.ETL_DICTIONARY_XMLNODE)
        except LookupError:
            return
        
        self.__replace_mode = dictionary_node.prop("replace")
        
        self.__if_mode = dictionary_node.prop("if")
        self.__if_mode = self.ETL_DICTIONARY_IF_MODE_CONTAINS if self.__if_mode is None else self.__if_mode
        
        database_node = util.getFirstChildNode(dictionary_node, self.ETL_DICTIONARY_DB_XMLNODE)
        database_host = util.getFirstChildNode(database_node, self.ETL_DICTIONARY_DB_HOST_XMLNODE).getContent()
        database_user = util.getFirstChildNode(database_node, self.ETL_DICTIONARY_DB_USER_XMLNODE).getContent()
        database_pass = util.getFirstChildNode(database_node, self.ETL_DICTIONARY_DB_PASS_XMLNODE).getContent()
        database_dbname = util.getFirstChildNode(database_node, self.ETL_DICTIONARY_DB_DBNAME_XMLNODE).getContent()
        connection = MySQLdb.connect (host = database_host, user = database_user, passwd = database_pass, db = database_dbname)
        
        sql_query = util.getFirstChildNode(database_node, self.ETL_DICTIONARY_DB_QUERY_XMLNODE).getContent()
        cursor = connection.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        
        columns = dict((name[0], col) for col, name in enumerate(cursor.description))
        map_from = columns["map_from"]
        map_to = columns["map_to"]
        
        self.__dictionary = []
        for row in rows:
            tokens = string.split(row[map_from], "|")
            self.__dictionary.extend(list((token, row[map_to]) for token in tokens))
            
        super(Dictionary, self).__init__(map_node)
        

    def isConfigured(self):
        return self.__dictionary is not None and len(self.__dictionary) > 0
    
    
    def processValue(self, data):
        for entry in self.__dictionary:
            start_index = None
            if(self.__if_mode == self.ETL_DICTIONARY_IF_MODE_CONTAINS):
                start_index = data.lower().find(entry[0].lower())
            
            if(self.__if_mode == self.ETL_DICTIONARY_IF_MODE_EQUALS):
                start_index = 0 if data.lower() == entry[0].lower() else -1
                
            if(start_index >= 0):
                if(self.__replace_mode == self.ETL_DICTIONARY_REPLACE_MODE_ALL):
                    return [entry[1], True]
                elif(self.__replace_mode == self.ETL_DICTIONARY_REPLACE_MODE_INLINE):
                    return [data.replace(entry[0], entry[1]), True]
                else:
                    return [data, False]
         
                
        return [data, False]   
        

class Trim(model.FieldProcessor):
    ETL_TRIM_XMLNODE = "trim"
    __side_mode = None
    
    def __init__(self, map_node):
        trim_node = None
        try:
            trim_node = util.getFirstChildNode(map_node, self.ETL_TRIM_XMLNODE)
        except LookupError:
            return
        
        self.__side_mode = trim_node.prop("side")
        
        super(Trim, self).__init__(map_node)
    
    def isConfigured(self):
        return self.__side_mode is not None and len(self.__side_mode) > 0
    
    def processValue(self, data):
        data = str(data)
        if(self.__side_mode == "left"):
            return [data.lstrip(), False]
        elif(self.__side_mode == "right"):
            return [data.rstrip(), False]
        else:
            return [data.strip(), False]
        
        
class SaveTo(model.FieldProcessor):
    ETL_SAVETO_XMLNODE = "save_to"
    __var_name = None
    
    def __init__(self, map_node):
        try:
            saveto_node = util.getFirstChildNode(map_node, self.ETL_SAVETO_XMLNODE)
        except LookupError:
            return
        
        self.__var_name = saveto_node.prop("variable")
        
        super(SaveTo, self).__init__(map_node)
    
    def isConfigured(self):
        return self.__var_name is not None
    
    def processValue(self, data):
        globals()[self.__var_name] = data
        return [data, True]        


class JoinWith(model.FieldProcessor):
    ETL_JOINWITH_XMLNODE = "join_with"
    __var_name = None
    
    def __init__(self, map_node):
        try:
            joinwith_node = util.getFirstChildNode(map_node, self.ETL_JOINWITH_XMLNODE)
        except LookupError:
            return
        
        self.__var_name = joinwith_node.prop("variable")
        
        super(JoinWith, self).__init__(map_node)
    
    def isConfigured(self):
        return self.__var_name is not None
    
    def processValue(self, data):
        var_value = globals()[self.__var_name]
        if(var_value is not None):
            return [var_value + data, True]
        else:
            return [data, True]


class Wrap(model.FieldProcessor):
    ETL_WRAP_XMLNODE = "wrap"
    __wrap_exp = None
    
    def __init__(self, map_node):
        try:
            wrap_node = util.getFirstChildNode(map_node, self.ETL_WRAP_XMLNODE)
        except LookupError:
            return
        
        self.__wrap_exp = wrap_node.prop("expression")
        
        super(Wrap, self).__init__(map_node)
    
    def isConfigured(self):
        return self.__wrap_exp is not None
    
    def processValue(self, data):
        if(data is not None):
            return [self.__wrap_exp.replace("%VALUE%", data), True]
        else:
            return [data, True]


class DateSufix(model.FieldProcessor):
    ETL_DATE_SUFIX_XMLNODE = "date_sufix"
    __date_sufix_format = None
    
    def __init__(self, map_node):
        try:
            date_sufix_node = util.getFirstChildNode(map_node, self.ETL_DATE_SUFIX_XMLNODE)
        except LookupError:
            return
        
        self.__date_sufix_format = date_sufix_node.prop("format")
        
        super(DateSufix, self).__init__(map_node)
    
    def isConfigured(self):
        return self.__date_sufix_format is not None
    
    def processValue(self, data):
        if(data is not None):
            sufix = datetime.datetime.today().strftime(self.__date_sufix_format)
            return [data + sufix, True]
        else:
            return [data, True]
