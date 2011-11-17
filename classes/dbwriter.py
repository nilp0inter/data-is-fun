#!/usr/bin/env python
"""Provee clases de escritores para utilizar desde el core.

Los escritores reciben diccionarios clave-valor y los escriben en 
distintos medios: bases de datos, xml, binario, etc.
"""


import sys
import logging

#
# apt-get install python-mysqldb
#
import MySQLdb

from table_maker import table_maker

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinez.es"

class dbwriter:
    """ Clase dbwriter. 
            Prepara y ejecuta queries contra la base de datos basadas en diccionarios.

    """

#    def __init__(self, hostname, database, user, password, \
#                table, strict_column_checking, skip_columns=[], \
#                pretend_queries=False, flexible_schema=False, \
#                force_text_fields=[]):
    def __init__(self, config):


        self.log = logging.getLogger('main.writer')

        self.hostname = config.get("writer", "hostname")
        self.database = config.get("writer", "database")
        self.username = config.get("writer", "username")
        self.password = config.get("writer", "password")
        self.table = config.get("writer", "table")


        self.force_text_fields = map(lambda x: x.strip(), config.get("writer", "force_text_fields", "string", "").split(","))
        if not self.force_text_fields:
            self.force_text_fields = []

        self.db = MySQLdb.connect(host=self.hostname, user=self.username, passwd=self.password, db=self.database)        
        self.cursor = self.db.cursor()

#        self.db = MySQLdb.connect(host=hostname, user=user,\
#                                  passwd=password, db=database)        
#
#        self.cursor = self.db.cursor()
#
#        self.table = table
#        self.strict_column_checking = strict_column_checking

        self.pretend_queries = config.get("writer", "pretend_queries", "boolean")
        if not self.pretend_queries:
            self.flexible_schema = config.get("writer", "flexible_schema", "boolean")
            self.force_text_fields = map(lambda x: x.strip(), config.get("writer", "force_text_fields", "string", "").split(","))
        else:
            self.flexible_schema = False
            self.force_text_fields = []
            self.log.warning("Writer will pretend queries, no changes will be made to database.")

        self.columns = {}

        self.strict_column_checking=config.get("writer", "strict_column_checking", "boolean")

        self.skip_columns=map(lambda x: x.strip(), config.get("writer", "skip_columns", "string", "").split(","))
        if type(self.skip_columns) != type([]):
            self.skip_columns = []

        # Load table schema and table_maker
        self.load_schemer()

        self.added = 0

        self.do_query("SET autocommit = 0")
        self.log.debug("Database writer started...")

    def load_schemer(self):
        try:
            self._get_column_info()
        except:
            if self.pretend_queries:
                self.log.critical('Pretend queries in non existant tables is useless')
                raise SystemExit
            # Table not found
            if self.flexible_schema:
                self.schema = table_maker(self.table, start_year=2011, end_year=2015, force_text_fields=self.force_text_fields)
                self.log.info("Creating table %s" % self.table)
                self.do_query(str(self.schema))

        if self.flexible_schema:
            self.schema = table_maker(self.table, start_year=2011, end_year=2015, force_text_fields=self.force_text_fields, fields = self.get_columns().values())
        
    def __del__(self):
        self.cursor.close()
        self.db.close()

    def do_query(self, sql):
        if self.pretend_queries:
            self.log.debug("Pretending query: %s" % sql)
        else:
            self.log.debug("Executing query: %s" % sql)

#            self.db.query(sql)        
            self.cursor.execute(sql)

    def do_rollback(self):
        if self.pretend_queries:
            self.log.info("Pretending rollback")
        else:
            self.log.info("Rollback")
            self.db.rollback()

    def do_commit(self):
        if self.pretend_queries:
            self.log.info("Pretending commit")
        else:
            self.log.info("Commit")
            self.db.commit()

    def get_columns(self):
        columns = self.columns.copy()
        for skip in self.skip_columns:
            try:
                columns.__delitem__(skip)
            except:
                pass
        return columns

    def _get_column_info(self):
        """
            Obtiene la lista de columnas de la base de datos.

        """
        self.db.query("SHOW COLUMNS FROM %s" % \
                      self.db.escape_string(self.table) )
        res = self.db.store_result()
        self.columns = {}
        while 1:
            row = res.fetch_row()
            if not row:
                break
            self.columns[row[0][0]] = row[0]

    def make_query(self, data, query_type = "insert", query_where = ""):
        """
            Forma consultas para los datos dados.
                query_type = "insert" | "update"
                query_where = clausula where, se puede usar sustitucion "%(nombrevariable)s"

        """

        self.added += 1

        if not data:
            return None

        if self.flexible_schema == True:
            prequery = self.schema.add_data(data)
            if prequery:
                if self.flexible_schema:
                    if prequery:
                        self.log.info("Adjusting %s column(s)..." % len(prequery))
                        for query in prequery:
                            self.do_query(query)
                        # Reload. Modified schema!
                        self.load_schemer()
                        return self.make_query(data, query_type, query_where)
                else:
                    self.log.error('Invalid column data type.')
                    raise ValueError

        if not query_type == "insert" and not query_type == "update":
            raise Exception("Unknown query type: %s" % query_type)

        columns = []
        setstrings = []

        if self.strict_column_checking and not self.columns.keys() == data.keys():
            self.log.debug("\nREGEXP COLUMNS: %s\nTABLE COLUMNS : %s" % (data.keys(),self.columns.keys()) )
            raise Exception("Columns mismatch! Fix regexp or set strict_column_checking=False.")

        # Recorre los valores de data y formatea dependiendo del tipo de consulta
        for key, value in data.iteritems():
            if key in self.skip_columns:
                continue

            if not key in self.columns.keys():
                self.log.debug("Skipping column in regexp: %s" % key)
                continue

            try:
                # Ugly... Get column datatype
                if self.columns[key][1].upper().find("VARCHAR") != -1 or \
                    self.columns[key][1].upper().find("TEXT") != -1 or \
                    self.columns[key][1].upper().find("BLOB") != -1:
                    quote_char = '"'
                else:
                    quote_char = ''
            except:
                quote_char = '"'

            columns.append("`" + key + "`")

            if value == "":
                value = "NULL"
                quote_char = ''

            if query_type == "insert":
                setstring = quote_char + self.db.escape_string(value) + quote_char
            elif query_type == "update":
                setstring = "`" + key + "` = " + quote_char + self.db.escape_string(value) + quote_char

            setstrings.append(setstring)

        # Transforma la variable query_where sustituyendo las variables por los valores de data
        if query_where:
            query_where = "WHERE " + query_where % data

        # Forma las consultas finales
        if query_type == "insert":
            query = "INSERT INTO %s (%s) VALUES (%s)" % (self.table, ", ".join(columns), ", ".join(setstrings))
        elif query_type == "update":
            query = "UPDATE %s SET %s %s" % (self.table, ", ".join(setstrings), query_where)

        return query

