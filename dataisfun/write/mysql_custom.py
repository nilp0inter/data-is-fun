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


#
# apt-get install python-mysqldb
#
import MySQLdb

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

from dataisfun.writers import Writer 

class mysql_custom(Writer):
    """ Clase mysql_custom. 
            Ejecuta querys mysql establecidas por el usuario.

    """


    def __init__(self, config, name):

        super(mysql_custom, self).__init__(config, name)

        self.hostname = self.config.get(self.name, "hostname")
        self.database = self.config.get(self.name, "database")
        self.username = self.config.get(self.name, "username")
        self.password = self.config.get(self.name, "password")
        self.query = self.config.get(self.name, "query")

        self.on_error = self.config.get(self.name, "on_error", "string", "rollback")

        self.pretend_queries = self.config.get(self.name, "pretend_queries", "boolean")
        if self.pretend_queries:
            self.log.warning("Writer will pretend queries, no changes will be made to database.")

    def start(self):
        self.db = MySQLdb.connect(host=self.hostname, user=self.username, passwd=self.password, db=self.database)
        self.cursor = self.db.cursor()
        self.added = 0
        self.do_query("SET autocommit = 0")

    def __del__(self):
        try:
            self.db.ping()
            self.finish()
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

    def add_data(self, data):
        try:
            sql_query = self.query % data
        except:
            if self.on_error != "pass":
                print self.query, data
                raise
            sql_query = False

        if sql_query:
            self.do_query(sql_query)
        elif self.on_error == "pass":
            self.log.warning("Invalid query, not inserting!")

    def finish(self):
        try:
            self.db
            self.do_commit()
        except:
            pass
        try:
            self.cursor.close()
        except:
            pass
        finally:
            try:
                self.db.close()
            except:
                pass


