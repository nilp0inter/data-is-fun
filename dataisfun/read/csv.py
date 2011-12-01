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


import csv
import copy

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

from dataisfun.readers import Reader

class csv(Reader):
    """
        Lector de csv.
    """

    def __init__(self, config, name, input_files):
       
        super(csv, self).__init__(config, name)

        if type(input_files) != list:
            self.original_input_files = [ input_files ]
        else:
            self.original_input_files = input_files


        self.reader = None

        self.dialect = self.config.get(self.name, "dialect", "string", None)

        try:
            self.fieldnames = map(lambda x: x.strip(), self.config.get(self.name, "fieldnames", "string", None).split(","))
        except:
            self.fieldnames = None
        try:
            self.restkey = map(lambda x: x.strip(), self.config.get(self.name, "restkey", "string", None).split(","))
        except:
            self.restkey = None
        try:
            self.restval = map(lambda x: x.strip(), self.config.get(self.name, "restval", "string", None).split(","))
        except:
            self.restval = None

        self.input_file = None

    def start(self):
        self.input_files = copy.copy(self.original_input_files)
        self._next_file()

    def next(self, extra_data = None):
        data = self.input_csv.next()
        if extra_data and data:
            data.update(extra_data)
        
        return data
            
        
    def finish(self):
        try:
            self.input_file.close()
        except:
            pass

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
        self.input_file = open(self.current_file, 'rb')
      
        if not self.dialect:
            dialect = csv.Sniffer().sniff(self.input_file.read(2048))
            self.input_file.seek(0)
        else:
            dialect = self.dialect

        self.input_csv = csv.DictReader(self.input_file, fieldnames=self.fieldnames, restkey=self.restkey, restval=self.restval, dialect=dialect)
        self.line_number = 0


