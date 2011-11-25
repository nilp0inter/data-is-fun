#!/usr/bin/env python
"""Clase de inspeccion de base de datos.

Esta clase simula una estructura de base de datos y recibe diccionarios
clave-valor al igual que lo hacen los escritores. Su mision es generar
excepciones si los datos que quieren ser introducidos en la base de 
datos no concuerdan con el tipo de dato de cada una de las columnas, a 
su vez provee las sentencias SQL necesarias para adaptar la tabla a las
necesidades de los datos.
"""

import logging

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinez.es"

from transformers import transform_factory

class field:
    def __init__(self, name, force_text=False, raise_on_change=True, \
                    mysql_definition=None, isnull=False):

        self.field_name = name
        self.isnull = isnull
        self.force_text = force_text
        self.raise_on_change = raise_on_change

        # Set field parameters from MySQL string
        if mysql_definition:
            self.mysql2field(mysql_definition)
        else:
            self.transformers =\
            transform_factory('transformers/',\
            force_output_writer = 'mysql', nullable = self.isnull) 

        self.field_type = self.transformers.get_best_definition()

    def mysql2field(self, mysql_field_string):
        import re
        regexp = re.compile("(?P<data_type>[A-Z]+)(?P<length>\(([0-9]+)(?:,([0-9]+))?\))?\s*(UNSIGNED)?.*?(?:\s(NULL|NOT NULL))?")
        # Improve
        #regexp.findall("VARCHAR(3,4) unsigned zerofill null".upper())
        #[('VARCHAR', '(3,4)', '3', '4', 'UNSIGNED', 'NULL')]
        res = regexp.findall(mysql_field_string.upper().strip()) 
        if res:
            field_type = res[0][0]
            field_unsigned = res[0][4]
            if field_unsigned:
                column_type = "_".join((field_unsigned.lower(), field_type.lower()))
            else:
                column_type = field_type.lower()
            if res[0][1] != "":
                if res[0][3]:
                    type_size = (int(res[0][2]), int(res[0][3]))
                else:
                    type_size = (int(res[0][2]),)
            else:
                type_size = None

            self.transformers =\
            transform_factory('transformers/',\
            force_output_type = column_type, force_output_writer = 'mysql',\
            nullable = self.isnull, type_size = type_size)
        else:
            print mysql_field_string.upper().strip()

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "`%s` %s" % (self.field_name, self.field_type)

    def add_value(self, value):
        changes = False

        if value == None:
            self.transformers.set_nullable()
        else:
            self.transformers.adjust(str(value))

        old_field_type = self.field_type

        self.field_type = self.transformers.get_best_definition()

        if old_field_type != self.field_type:
            changes = True

        if self.raise_on_change and changes:
                raise ValueError

    def transform(self, value):
        return self.transformers.transform(value)

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
                not_used_fields.remove(key)
            except:
                pass

            # New column
            if not key in self.fields.keys():
                self.log.warning("Column `%s` needs to be created" % key) 
                self.fields[key] = field(key, force_text = (key in self.force_text_fields)) 
                res.append("ALTER TABLE %s ADD %s" % (self.table_name, self.fields[key]))
                self.last_changes[key] = {}
                self.last_changes[key]['CREATE'] = "ALTER TABLE %s ADD %s" % (self.table_name, self.fields[key])
            try:
                self.fields[key].add_value(value)
            except:
                if self.alter_table:
                    self.log.warning("Column `%s` needs to be adjusted (%s)" % (key, self.fields[key])) 
                    res.append("ALTER TABLE %s MODIFY %s" % (self.table_name, self.fields[key]))
                    self.last_changes[key]['MODIFY'] = "ALTER TABLE %s MODIFY %s" % (self.table_name, self.fields[key])
           
        # Set null not used columns
        for key in not_used_fields:
            try:
                self.fields[key].add_value(None)
            except:
                if self.alter_table:
                    self.log.warning("Column `%s` needs to be adjusted (NULL)" % key) 
                    res.append("ALTER TABLE %s MODIFY %s" % (self.table_name, self.fields[key]))
                    self.last_changes[key]['MODIFY'] = "ALTER TABLE %s MODIFY %s" % (self.table_name, self.fields[key])
             
        return res
    def create_table(self):
        fields = ""
        for value in self.fields.values():
            if fields != "":
                fields = "%s,%s" % (fields, str(value))
            else:
                fields = str(value)
        partitions = """
"""
        table = """CREATE TABLE  `%s` (
%s
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_spanish_ci
%s
""" % (self.table_name, fields, partitions)
        return table

    def __str__(self):
        return self.create_table()

