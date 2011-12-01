#!/usr/bin/env python
#
#    Copyright (C) 2011 Roberto A. Martinez Perez
#
#    This file is part of data-is-fun.
#
#    data-is-fun is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    data-is-fun is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with data-is-fun.  If not, see <http://www.gnu.org/licenses/>.

"""Provee clases de escritores para utilizar desde el core.

Los escritores reciben diccionarios clave-valor y los escriben en 
distintos medios: bases de datos, xml, binario, etc.
"""


import MySQLdb

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

from dataisfun.writer import Writer
from dataisfun.util import table_maker

class mysql(Writer):
    """ Clase mysqlwriter. 
            Prepara y ejecuta queries contra mysql.

    """


    def __init__(self, config, name):
        #
        # apt-get install python-mysqldb
        #
        #import MySQLdb
        #from table_maker import table_maker

        super(mysql, self).__init__(config, name)

        self.hostname = self.config.get(self.name, "hostname")
        self.database = self.config.get(self.name, "database")
        self.username = self.config.get(self.name, "username")
        self.password = self.config.get(self.name, "password")
        self.table = self.config.get(self.name, "table")

        self.force_text_fields = map(lambda x: x.strip(), self.config.get(self.name, "force_text_fields", "string", "").split(","))
        if not self.force_text_fields:
            self.force_text_fields = []

        self.pretend_queries = self.config.get(self.name, "pretend_queries", "boolean")
        if not self.pretend_queries:
            self.flexible_schema = self.config.get(self.name, "flexible_schema", "boolean")
            self.force_text_fields = map(lambda x: x.strip(), self.config.get(self.name, "force_text_fields", "string", "").split(","))
        else:
            self.flexible_schema = False
            self.force_text_fields = []
            self.log.warning("Writer will pretend queries, no changes will be made to database.")

        self.columns = {}
        self.strict_column_checking=self.config.get(self.name, "strict_column_checking", "boolean")

        self.query_type = self.config.get(self.name, "query_type", "string", "insert")
        self.query_where = self.config.get(self.name, "query_where", "string", "")

        self.skip_columns=map(lambda x: x.strip(), self.config.get(self.name, "skip_columns", "string", "").split(","))
        if type(self.skip_columns) != type([]):
            self.skip_columns = []


    def start(self):
        self.db = self._mysqldb.connect(host=self.hostname, user=self.username, passwd=self.password, db=self.database)
        self.cursor = self.db.cursor()

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
                self.schema = self._table_maker.table_maker(self.table, start_year=2011, end_year=2015, force_text_fields=self.force_text_fields)
                self.log.info("Creating table %s" % self.table)
                self.do_query(str(self.schema))

        self.schema = self._table_maker.table_maker(self.table, start_year=2011, end_year=2015, force_text_fields=self.force_text_fields, fields = self.get_columns().values())
        
    def __del__(self):
        try:
            self.cursor.close()
        except:
            pass
        finally:
            try:
                self.db.close()
            except:
                pass

    def do_query(self, sql):
        if self.pretend_queries:
            self.log.debug("Pretending query: %s" % sql)
        else:
            self.log.debug("Executing query: %s" % sql)
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

    def make_query(self, data):
        """
            Forma consultas para los datos dados.

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
                        return self.make_query(data, self.query_type, self.query_where)
                else:
                    self.log.error('Invalid column data type.')
                    raise ValueError

        if not self.query_type == "insert" and not self.query_type == "update":
            raise Exception("Unknown query type: %s" % self.query_type)

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

            columns.append("`" + key + "`")

            if self.query_type == "insert":
                formatted_value = self.schema.fields[key].transform(value)
                if formatted_value == None:
                    formatted_value = 'NULL'
                setstring = str(formatted_value)
            elif self.query_type == "update":
                formatted_value = self.schema.fields[key].transform(value)
                if formatted_value == None:
                    formatted_value = 'NULL'

                setstring = "`%s` = %s" % (key, self.schema.fields[key].transform(value) )

            setstrings.append(setstring)

        # Transforma la variable query_where sustituyendo las variables por los valores de data
        if self.query_where:
            query_where = "WHERE " + self.query_where % data

        # Forma las consultas finales
        if self.query_type == "insert":
            query = "INSERT INTO %s (%s) VALUES (%s)" % (self.table, ", ".join(columns), ", ".join(setstrings))
        elif self.query_type == "update":
            query = "UPDATE %s SET %s %s" % (self.table, ", ".join(setstrings), query_where)

        return query


    def add_data(self, data):
        self.log.debug("Reader (%s) receive data: %s" % (self.name, data))
        sql_query = self.make_query(data)
        if sql_query:
            self.do_query(sql_query)
        elif self.on_error == "pass":
            self.log.warning("Invalid query, not inserting! Maybe malformed regexp or malformed line?")
        else:
            raise Exception("Empty query!, maybe malformed regexp?")

    def finish(self):
        self.do_commit()


