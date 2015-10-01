import model
import util
import MySQLdb #Se requiere instalar
import sys

class TargetProcessor(object):
    __registered = ["TargetDatabase"]
    __configuredTarget = None
        
    def __init__(self, map_context):
        # Configurando el destino de los datos
        target_nodes = map_context.xpathEval("/map/target")
        if(target_nodes is not None):
            for clazz in self.__registered:
                constructor = getattr(sys.modules[__name__], clazz) 
                target = constructor(target_nodes[0])
                
                if(target.isConfigured()):
                    self.__configuredTarget = target
    
    def getConfiguredProcessor(self):
        if(self.__configuredTarget is None):
            raise AssertionError("Target no configurado")
        
        return self.__configuredTarget


class TargetDatabase(model.RowProcessor):
    
    ETL_TARGET_DB_MODE = 1
    ETL_TARGET_DB_XMLNODE = "database"
    ETL_TARGET_DB_HOST_XMLNODE = "host"
    ETL_TARGET_DB_USER_XMLNODE = "user"
    ETL_TARGET_DB_PASS_XMLNODE = "pass"
    ETL_TARGET_DB_DBNAME_XMLNODE = "dbname"
    ETL_TARGET_DB_DEFAULT_TABLE_XMLNODE = "default_table"
    ETL_TARGET_DB_KEYS_XMLNODE = "keys"
    ETL_TARGET_DB_VALIDATION_XMLNODE = "validation"
    
    fields = {}
    __connection = None
    __default_table = None
    __keys = None
    __validation = None
    __key_field_id = None
    __data_seq = []
    __row_mapping = {}
    
    def __init__(self, map_node):
        target_node = util.getFirstChildNode(map_node, self.ETL_TARGET_DB_XMLNODE)
        database_host = util.getFirstChildNode(target_node, self.ETL_TARGET_DB_HOST_XMLNODE).getContent()
        database_user = util.getFirstChildNode(target_node, self.ETL_TARGET_DB_USER_XMLNODE).getContent()
        database_pass = util.getFirstChildNode(target_node, self.ETL_TARGET_DB_PASS_XMLNODE).getContent()
        database_dbname = util.getFirstChildNode(target_node, self.ETL_TARGET_DB_DBNAME_XMLNODE).getContent()
        self.__connection = MySQLdb.connect (host = database_host, user = database_user, passwd = database_pass, db = database_dbname)
        
        self.__default_table = util.getFirstChildNode(target_node, self.ETL_TARGET_DB_DEFAULT_TABLE_XMLNODE).getContent()
        self.__keys = util.getFirstChildNode(target_node, self.ETL_TARGET_DB_KEYS_XMLNODE).getContent()
        
        try:
            self.__validation = util.getFirstChildNode(target_node, self.ETL_TARGET_DB_VALIDATION_XMLNODE).getContent()
        except LookupError:
            pass

    def mode(self):
        return self.ETL_TARGET_DB_MODE
    
    def isConfigured(self):
        return self.__connection is not None
    
    def confField(self, field_id, map_node):
        field_node = util.getFirstChildNode(map_node, self.ETL_FIELD_TARGET_XMLNODE)
        
#        try:
        field = FieldTargetDatabase(field_node)
        self.fields[field_id] = field
        
        if(field.getName() == self.__keys):
            self.__key_field_id = field_id
                
#        except AssertionError:
#            self.fields[field_id] = model.FieldProcessor(field_node)
            

    def nextRow(self):
        if(len(self.__row_mapping) > 0):
            self.__data_seq.append(self.__row_mapping)
            
        self.__row_mapping = {}
        return True
    
    def dropRow(self):
        self.__row_mapping = {}

    def processField(self, field_id, data):
        field = self.fields[field_id]
        value = field.processValue(data)
        
#        if(type(field).__name__ == "FieldTargetDatabase"):
        self.__row_mapping[field.getName()] = value[0]
        
        is_valid = field.validate(value[0])
        
        return field.postProcessValue(value[0])

    def flush(self):
#        Solo una intencion...
#        query = "INSERT INTO `" + self.__default_table + "` ("
#        query += ",".join(list(field.attrs[0] for field in self.fields.itervalues()))
#        query += ") VALUES ("
#        query += ",".join(list("%(" + field.attrs[0] + ")s" for field in self.fields.itervalues()))
#        query += ")"
#        util.dump(self.__data_seq)

        fields_butkeys = self.fields.copy()
        key_field = fields_butkeys.pop(self.__key_field_id)

        query = "UPDATE `" + self.__default_table + "` SET "
        query += ", ".join(
                           list(field.attrs[0] + " = " 
                                + ( "STR_TO_DATE(%(" + field.attrs[0] + ")s, '" + field.attrs[1].replace("%", "%%") + "')" if field.attrs[1] is not None 
                                    else "%(" + field.attrs[0] + ")s") 
                                for field in fields_butkeys.itervalues()))
        query += " WHERE " + self.__keys + " = %(" + key_field.attrs[0] + ")s" + (" AND " + self.__validation if self.__validation is not None else "")
        
#        Esta es la alternativa ultra-rapida, aqui el etl opera con "resolucion"... depende de la cantidad de veces que
#        se ejecute, la cantidad de info que se genera...
#        query = "UPDATE `" + self.__default_table + "` SET "
#        for field in fields_butkeys.itervalues():
#            field_name = field.attrs[0]
#            query += field_name + " = CASE " + self.__keys + " "
#            
#            for row in self.__data_seq:
#                query += "WHEN " + str(row[self.__keys]) + " THEN '" + str(row[field_name]) + "' \n"
#                
#            query += "ELSE " + field_name + " END,"
#        
#        query = query[:-1]
#        
#        query += " WHERE id IN (" + ",".join(list(str(row[self.__keys]) for row in self.__data_seq)) + ")" 
                              
#        print query
        cursor = self.__connection.cursor()
        
        cursor.execute("SET GLOBAL key_buffer_size = 100663296")
        cursor.execute("SET GLOBAL max_allowed_packet = 16777216")
        cursor.executemany(query, self.__data_seq)
        self.__connection.commit()

#        cursor.execute("LOCK TABLES espira WRITE, hist_espira WRITE")
#        cursor.execute(query)
#        cursor.execute("UNLOCK TABLES")
        
        cursor.close()



class FieldTargetDatabase(model.FieldProcessor):
    ETL_FIELD_TARGET_DATABASE_XMLNODE = "column"
    ETL_FIELD_TARGET_DATABASE_ATTR_DF = "date_format"
    
    def __init__(self, map_node):
        try:
            column_node = util.getFirstChildNode(map_node, self.ETL_FIELD_TARGET_DATABASE_XMLNODE)

            if(column_node.prop(self.ETL_FIELD_TARGET_DATABASE_ATTR_DF) is not None):
                df_prop = column_node.prop(self.ETL_FIELD_TARGET_DATABASE_ATTR_DF)
            else:
                df_prop = None
                
            self.attrs = [column_node.getContent(), df_prop]
        except LookupError:
            raise AssertionError("Campo target no configurado")
#        
#        super(FieldTargetDatabase, self).__init__(map_node)
    
    def getName(self):
#        if(self.attrs is None):
#            return None
#        else:
        return self.attrs[0]
    
    def isConfigured(self):
        return self.attrs is not None

    def processValue(self, data):
#        if(self.attrs is None):
#            return super(FieldTargetDatabase, self).processValue(data)
#        else:
        return [data, True]