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

"""Lectores para text2db.

Estas clases reciben interpretan datos desde distintos medios y los transforman
en diccionarios clave valor para enviarlos al core. Deben ser iteradores ya que 
el core los tratara como tal.
"""


import sqlite3
import copy

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

from dataisfun.readers import Reader

class sqlite(Reader):
    """
        Lector de sqlite.
        Se le especifica la query (puede utilizarse sustitucion de variables).
        Mientras la query no cambie se recorren los resultados obtenidos hasta 
        su final.
    """

    def __init__(self, config, name, input_files):
       
        super(sqlite, self).__init__(config, name)

        self.query = self.config.get(self.name, "query")

        if type(input_files) != list:
            self.original_input_files = [ input_files ]
        else:
            self.original_input_files = input_files

        self.requery = self.config.get(self.name, "requery", "boolean", False)
        self.last_query = None
        self.input_file = None

    def dict_factory(self, cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def do_query(self, sql):
        self.log.debug("Executing query: %s" % sql)
        self.cursor.execute(sql)
        self.step_max = self.cursor.rowcount

    def start(self):
        super(sqlite, self).start()
        if self.requery:
            self.overall_max = -1
        else:
            self.overall_max = 1
            self.overall_current = 1

        self._next_file()


    def _next_file(self):

        try:
            self.input_files
        except:
            self.input_files = copy.copy(self.original_input_files)

        if self.input_files:
            self.current_file = self.input_files.pop()
        elif self.cyclic:
            self.input_files = copy.copy(self.original_input_files)
            self.current_file = self.input_files.pop()
        else:
            raise StopIteration

        if self.input_file:
            self.input_file.close()

        self.log.debug("Opening file (%s)" % self.current_file)
      
        self.db = sqlite3.connect(self.current_file)
        self.db.row_factory=self.dict_factory
        self.cursor = self.db.cursor()
        self.step_current = 0

    def next(self, extra_data = None):
        try:
            self.current_query = self.query % extra_data
        except TypeError:
            self.current_query = self.query

        if self.current_query != self.last_query or self.requery:
            try:
                self.cursor.close()
            except:
                pass
            finally:
                self.cursor = self.db.cursor()

            self.do_query(self.current_query)
            self.last_query = self.current_query

        data = self.cursor.fetchone()

        if not data:
            self._next_file()
            return self.next(extra_data)

        elif extra_data:
            extra_data.update(data)
            data=extra_data

        self.step_current += 1
        return data
            
    def finish(self):
        del self.input_files
        self.cursor.close()
        self.db.close()
        self.last_query = None

