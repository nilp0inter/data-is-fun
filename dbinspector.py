#!/usr/bin/env python
"""Clase de inspección de base de datos.

Interfaz para table_maker, dependiendo de la configuración crea o modifica
tablas de MySQL.
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
__email__ = "contacto@robertomartinezp.es"
__status__ = "Production"

class dbinspector:
    """
        Clase dbinspector. 
            Crea tablas de MySQL.

    """

    def __init__(self, hostname, database, user, password, table, \
                skip_columns=[], force_text_fields = []):


        self.log = logging.getLogger('main.writer')
        self.db = MySQLdb.connect(host=hostname, user=user, passwd=password, db=database)        

        self.cursor = self.db.cursor()

        self.table = table
        if force_text_fields:
            self.force_text_fields = force_text_fields
        else:
            self.force_text_fields = []

        self.must_create = False
        self.skip_columns = skip_columns

        self.columns = {}

        self.load_schemer()

        self.do_query("SET autocommit = 0")
        self.log.debug("Database inspector started...")

        self.pretend_queries = False

        self.added = 0

    def load_schemer(self):
        try:
            self._get_column_info()
        except:
            # Table not found
            self.must_create = True

        self.schema = table_maker(self.table, start_year=2011, end_year=2015, force_text_fields=self.force_text_fields, fields = self.get_columns().values())
        
    def __del__(self):
        self.cursor.close()
        self.db.close()

    def do_query(self, sql):
        self.log.debug("Executing query: %s" % sql)
        self.cursor.execute(sql)

    def do_commit(self):
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

    def add_data(self, data):

        self.added += 1

        if not data:
            return None

        for skip in self.skip_columns:
            try:
                data.__delitem__(skip)
            except:
                pass
        self.schema.add_data(data)
        return None

    def finish(self):
        if self.must_create:
            self.log.info("Creating table %s ..." % self.table)
            self.do_query(str(self.schema)) 
        else:
            for key, value in self.schema.last_changes.iteritems():
                if value.has_key('CREATE'):
                    self.log.info("Adding column `%s`" % key)
                    self.do_query(value['CREATE']) 
                if value.has_key('MODIFY'):
                    self.log.info("Changing column `%s`" % key)
                    self.do_query(value['MODIFY']) 
