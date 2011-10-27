import logging

class field:
    def __init__(self, name, force_text=False, raise_on_change=True, \
                    mysql_definition=None, isnull=False):

        self.field_name = name
        self.field_type = None # INT, DECIMAL, VARCHAR

        self.max_value = None
        self.min_value = None
        self.max_length = None
        self.isnull = isnull

        self.force_text = force_text
        self.raise_on_change = raise_on_change

        self.r_unsigned = ""
        self.r_null = ""
        self.r_type = ""

        # Set field parameters from MySQL string
        if mysql_definition:
            self.mysql2field(mysql_definition)

    def mysql2field(self, mysql_field_string):
        import re
        regexp = re.compile("([A-Z]+)(\(([0-9]+)(,([0-9]+))?\))?(\s+([A-Z]+))?")
        # >>> r.findall("VARCHAR(3,70) unsigned")
        # [('VARCHAR', '(3,70)', '3', ',70', '70')]

        res = regexp.findall(mysql_field_string.upper().strip()) 
        if res:
            field_type=res[0][0]
            field_size=res[0][2]
            field_decimal=res[0][4]
            self.r_unsigned=res[0][6]
            if field_type in ('TINYINT','SMALLINT','MEDIUMINT','INT','BIGINT'):
                self.field_type = 'INT'

                if self.r_unsigned:
                    self.min_value = 0

                    self.r_unsigned = "UNSIGNED" 

                    if field_type == "TINYINT":
                        self.max_value = 255
                    elif field_type == "SMALLINT": 
                        self.max_value = 65535
                    elif field_type == "MEDIUMINT":
                        self.max_value = 16777215
                    elif field_type == "INT":
                        self.max_value = 4294967295
                    else: # BIGINT
                        self.max_value = 18446744073709551615
                else:
                    self.r_unsigned = "" 
                    if field_type == "TINYINT":
                        self.max_value = 127
                        self.min_value = -128
                    elif field_type == "SMALLINT": 
                        self.max_value = 32767
                        self.min_value = -32768
                    elif field_type == "MEDIUMINT":
                        self.max_value = 8388607
                        self.min_value = -8388608
                    elif field_type == "INT":
                        self.max_value = 2147483647
                        self.min_value = -2147483648
                    else: # BIGINT
                        self.max_value = 9223372036854775807
                        self.min_value = -9223372036854775808
            elif field_type in ('VARCHAR', 'TINYTEXT', 'TEXT', 'BLOB', 'MEDIUMTEXT', 'MEDIUMBLOB', 'LONGTEXT', 'LONGBLOB'):
                # http://www.htmlite.com/mysql003.php
                self.field_type = 'VARCHAR'
                self.r_unsigned = ""
                if field_type == "VARCHAR":
                    if field_size:
                        try:
                            self.max_length = int(field_size)
                        except:
                            self.max_length = 255
                    else:
                        self.max_length = 1
                elif field_type == "TINYTEXT":
                    self.max_length = 255
                elif field_type == "TEXT" or field_type == "BLOB":
                    self.max_length = 65535
                elif field_type == "MEDIUMTEXT" or field_type == "MEDIUMBLOB":
                    self.max_length = 16777215
                elif field_type == "LONGTEXT" or field_type == "MEDIUMBLOB":
                    self.max_length = 4294967295
            elif field_type in ('FLOAT', 'DOUBLE', 'DECIMAL'):
                # We ever use DECIMAL because it has fixed decimal point, exact values.
                self.field_type = 'DECIMAL'
                if self.r_unsigned:
                    self.r_unsigned = "UNSIGNED"
                else:
                    self.r_unsigned = ""
                
                if field_size and field_decimal:
                    # Ugly... we build up strings like 99.9999 ~ -99.9999 to define decimals
                    self.max_value = ('9'*(int(field_size)-int(field_decimal))) + '.' + ('9'*(int(field_decimal)))
                    self.min_value = '-' + ('9'*(int(field_size)-int(field_decimal))) + '.' + ('9'*(int(field_decimal)))
                elif field_size: 
                    self.max_value = ('9'*(int(field_size)))
                    self.min_value = '-' + ('9'*(int(field_size)))
              
    def isint(self, value):
        try:
            value = int(value)
            return True
        except:
            return False
           
    def isfloat(self, value):
        try:
            value = float(value)
            return True
        except:
            return False 

    def _set_type(self, type_name):

        if self.force_text:
            self.field_type = "VARCHAR"

        if self.field_type == None:
            self.field_type = type_name
        elif self.field_type == "INT" and type_name != "INT":
            self.field_type = type_name
        elif self.field_type == "DECIMAL" and type_name == "VARCHAR":
            self.type_name = type_name
        else:
            pass

    def _render(self):
        
        if self.isnull:
            self.r_null = "NULL"
        else:
            self.r_null = "NOT NULL"

        if self.field_type == "INT":
            if self.min_value >= 0:
                self.r_unsigned = "UNSIGNED" 
                if self.max_value <= 255:
                    self.r_type = "TINYINT"
                elif self.max_value <= 65535:
                    self.r_type = "SMALLINT"
                elif self.max_value <= 16777215:
                    self.r_type = "MEDIUMINT"
                elif self.max_value <= 4294967295:
                    self.r_type = "INT"
                elif self.max_value <= 18446744073709551615:
                    self.r_type = "BIGINT"
                else:
                    self.field_type = "VARCHAR"
                    #raise Exception("Value %s or %s is too long for mysql" % (self.min_value, self.max_value))
            else:
                if -128 <= self.min_value <= 0 <= self.max_value <= 127:
                    self.r_unsigned = "" 
                    self.r_type = "TINYINT"
                elif -32768 <= self.min_value <= 0 <= self.max_value <= 32767:
                    self.unsigned = "" 
                    self.r_type = "SMALLINT"
                elif -8388608 <= self.min_value <= 0 <= self.max_value <= 8388607:
                    self.unsigned = "" 
                    self.r_type = "MEDIUMINT"
                elif -2147483648 <= self.min_value <= 0 <= self.max_value <= 2147483647:
                    self.unsigned = "" 
                    self.r_type = "INT"
                elif -9223372036854775808 <= self.min_value <= 0 <= self.max_value <= 9223372036854775807:
                    self.unsigned = "" 
                    self.r_type = "BIGINT"
                else:
                    self.r_unsigned = "" 
                    self.field_type = "VARCHAR"
                    #raise Exception("Value %s or %s is too long for mysql" % (self.min_value, self.max_value))

        if self.field_type == "DECIMAL":
            try:
                decimal_min = len(str(self.min_value).split('.')[0])-1
            except:
                decimal_min = 1

            try:
                decimal_max = len(str(self.max_value).split('.')[0])
            except:
                decimal_max = 1
            
            try:
                mantissa_min = len(str(self.min_value).split('.')[1])
            except:
                mantissa_min = 0

            try:
                mantissa_max = len(str(self.max_value).split('.')[1])
            except:
                mantissa_max = 0

            decimal = max(decimal_min, decimal_max) 
            mantissa = max(mantissa_min, mantissa_max) 

            self.r_type = "DECIMAL(%s,%s)" % ((decimal+mantissa), mantissa)
        
        if self.field_type == "VARCHAR":
            self.r_unsigned = ""
            if self.max_length <= 255:
                self.r_type = "VARCHAR(%s)" % self.max_length
            elif self.max_length <= 65535:
                self.r_type = "TEXT"
            elif self.max_length <= 16777215:
                self.r_type = "MEDIUMTEXT"
            elif self.max_length <= 4294967295:
                self.r_type = "LONGTEXT"
            else:
                raise Exception("Text value too long for mysql")

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        self._render()
        if self.field_type == None or self.field_name == None:
            return ""
        return "`%s` %s %s %s" % (self.field_name, self.r_type, \
                                    self.r_unsigned, self.r_null)
    def add_value(self, value):

        changes = False
        if self.raise_on_change:
            old_def = self.__str__()

        if value == None or value == "":
            self.isnull = True
            if self.raise_on_change and old_def != self.__str__():
               raise ValueError
            return

        value = str(value)
        # Set max length of (string)value
        if self.max_length == None:
            self.max_length = len(value)
            changes = True
        else:
            old_value = self.max_length
            self.max_length = max(len(value), self.max_length)
            if old_value != self.max_length:
                changes = True

        if self.isint(value):
            value_type = "INT"
            try:
                old_value = int(self.max_value)
            except:
                old_value = 0
            try:
                new_value = int(value)
            except:
                # :S
                new_value = 0
            self.max_value = max(old_value, new_value)
            if old_value != self.max_value:
                changes = True
            try:
                old_value = int(self.min_value)
            except:
                old_value = 0
            try:
                new_value = int(value)
            except:
                # :S
                new_value = 0
            self.min_value = min(old_value, new_value)
            if old_value != self.max_value:
                changes = True
        elif self.isfloat(value):
            value_type = "DECIMAL"
            try:
                old_value = float(self.max_value)
            except:
                old_value = 0
            try:
                new_value = float(value)
            except:
                # :S
                new_value = 0
            self.max_value = max(old_value, new_value)
            if old_value != self.max_value:
                changes = True
            try:
                old_value = float(self.min_value)
            except:
                old_value = 0
            try:
                new_value = float(value)
            except:
                # :S
                new_value = 0
            self.min_value = min(old_value, new_value)
            if old_value != self.max_value:
                changes = True
        else:
            value_type = "VARCHAR"

        old_value = self.field_type
        self._set_type(value_type)
        if old_value != self.field_type:
            changes = True

        if self.raise_on_change and changes and old_def != self.__str__():
                raise ValueError

class table_maker:
    """
        Clase table_maker.
            Analiza los datos pasados y genera un esquema de base de datos InnoDB 
            con particionado.

    """
            
    def __init__(self, table_name, start_year=2000, end_year=2050, force_text_fields=[], fields=None, alter_table=True):
        self.table_name = table_name
        self.start_year = start_year
        self.end_year = end_year 
        self.alter_table = alter_table 
        self.force_text_fields = force_text_fields

        self.last_changes = {}

        if not fields:
            self.fields = {}
        else:
            # Load database fields
            #   field_name,  field_type, isnull, 'n/a', default, 'n/a'
            # (('data_date', 'date', 'NO', '', '0000-00-00', ''),)
            # (('campo1', 'varchar(255)', 'NO', '', None, ''),)
            self.fields = {}
            for f in fields:
                if f[2] == 'NO':
                    isnull = False
                else:
                    isnull = True
                self.fields[f[0]] = field(f[0], mysql_definition=f[1], force_text = (f[0] in self.force_text_fields), isnull=isnull)
                self.last_changes[f[0]] = {}


        self.log = logging.getLogger('main.table_maker')

                
    def add_data(self, data):
        if not data:
            return

        not_used_fields = self.fields.keys()
        # Consultas SQL que se han de ejecutar en la base de datos para modificar las columnas
        #  necesarias antes de la insercion. (self.alter_table==True)
        res = []

        for key, value in data.iteritems():
            try:
                not_used_fields.__delitem__(key)
            except:
                pass

            # New column
            if not key in self.fields.keys():
                self.log.warning("Column `%s` needs to be created" % key) 
                self.fields[key] = field(key, force_text = (key in self.force_text_fields), mysql_definition="tinyint null") 
                res.append("ALTER TABLE %s ADD %s" % (self.table_name, self.fields[key]))
                self.last_changes[key] = {}
                self.last_changes[key]['CREATE'] = "ALTER TABLE %s ADD %s" % (self.table_name, self.fields[key])

            try:
                self.fields[key].add_value(value)
            except:
                if self.alter_table:
                    self.log.warning("Column `%s` needs to be adjusted" % key) 
                    res.append("ALTER TABLE %s MODIFY %s" % (self.table_name, self.fields[key]))
                    self.last_changes[key]['MODIFY'] = "ALTER TABLE %s MODIFY %s" % (self.table_name, self.fields[key])
           
        # Set null not used columns
        for key in not_used_fields:
            try:
                self.fields[key].add_value("")
            except:
                if self.alter_table:
                    self.log.warning("Column `%s` needs to be adjusted" % key) 
                    res.append("ALTER TABLE %s MODIFY %s" % (self.table_name, self.fields[key]))
                    self.last_changes[key]['MODIFY'] = "ALTER TABLE %s MODIFY %s" % (self.table_name, self.fields[key])
             
        return res

    def create_table(self):
        fields = ""
        for value in self.fields.values():
            if field != "":
                fields = "%s,\n%s" % (fields, str(value))
            else:
                fields = str(value)

        partitions = """/*!50100 PARTITION BY RANGE (YEAR(data_date))
SUBPARTITION BY HASH (MONTH(data_date))
("""
        for year in range(self.start_year, self.end_year):
            this_year = year - 1
            partitions = partitions + """PARTITION p%(this_year)s VALUES LESS THAN (%(year)s)
                (SUBPARTITION s_dec_%(this_year)s ENGINE = InnoDB,
                  SUBPARTITION s_jan_%(this_year)s ENGINE = InnoDB,
                  SUBPARTITION s_feb_%(this_year)s ENGINE = InnoDB,
                  SUBPARTITION s_mar_%(this_year)s ENGINE = InnoDB,
                  SUBPARTITION s_apr_%(this_year)s ENGINE = InnoDB,
                  SUBPARTITION s_may_%(this_year)s ENGINE = InnoDB,
                  SUBPARTITION s_jun_%(this_year)s ENGINE = InnoDB,
                  SUBPARTITION s_jul_%(this_year)s ENGINE = InnoDB,
                  SUBPARTITION s_aug_%(this_year)s ENGINE = InnoDB,
                  SUBPARTITION s_sep_%(this_year)s ENGINE = InnoDB,
                  SUBPARTITION s_oct_%(this_year)s ENGINE = InnoDB,
                  SUBPARTITION s_nov_%(this_year)s ENGINE = InnoDB)""" % dict( (('this_year', this_year),('year', year)) ) 
            if year != (self.end_year - 1):
                partitions = partitions + ",";
            else:
                partitions = partitions + ")*/";
            
        return """CREATE TABLE  `%s` (
`data_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP %s
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_spanish_ci
%s
""" % (self.table_name, fields, partitions)

    def __str__(self):
        return self.create_table()

