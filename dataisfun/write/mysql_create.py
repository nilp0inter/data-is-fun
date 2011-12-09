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

from dataisfun.writers import Writer 
from dataisfun.util import table_maker

class mysql_create(Writer):
    """
        Clase MySqlInspector. 
            Crea tablas de MySQL.

    """

    def __init__(self, config, name):

        super(mysql_create, self).__init__(config, name)

        self.hostname = self.config.get(self.name, "hostname")
        self.database = self.config.get(self.name, "database")
        self.username = self.config.get(self.name, "username")
        self.password = self.config.get(self.name, "password")
        self.table = self.config.get(self.name, "table")

        self.skip_columns=map(lambda x: x.strip(), self.config.get(self.name, "skip_columns", "string", "").split(","))

        self.force_text_fields = map(lambda x: x.strip(), self.config.get(self.name, "force_text_fields", "string", "").split(","))
        if not self.force_text_fields:
            self.force_text_fields = []

        self.must_create = False
        self.columns = {}
        self.pretend_queries = False
        self.added = 0


    def start(self):
        self.db = MySQLdb.connect(host=self.hostname, user=self.username, passwd=self.password, db=self.database)
        self.cursor = self.db.cursor()
        self.load_schemer()
        self.do_query("SET autocommit = 0")
        self.log.debug("Database inspector started...")

    def load_schemer(self):
        try:
            self._get_column_info()
        except:
            # Table not found
            self.must_create = True

        self.schema = table_maker.table_maker(self.table, start_year=2011, end_year=2015, force_text_fields=self.force_text_fields, fields = self.get_columns().values())
        
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
