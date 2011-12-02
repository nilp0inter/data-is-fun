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


import os
import re
import copy

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

from dataisfun.readers import Reader
    
class regexp(Reader):
    """
        Clase reader (iterable). Recibe un fichero o nombre
         de fichero y una expresion regular. Parsea cada linea
         y devuelve el diccionario de valores parseados.
    """

    def __init__(self, config, name, input_files):
       
        super(regexp, self).__init__(config, name)

        _regexp = self.config.get(self.name, "regexp")

        if type(input_files) != list:
            self.original_input_files = [ input_files ]
        else:
            self.original_input_files = input_files

        try:
            self._regexp = [re.compile(_regexp)]
            self.long_regexp = False
        except:
            # Python no soporta mas de 100 grupos nominales.
            # Como "probablemente" estemos utilizando una expresion regular para capturar
            # campos de ancho fijo, generaremos N expresiones regulares con un maximo 
            # de 100 campos nominales cada una
            
            self.long_regexp = True
            self._regexp = []
            splitregexp = re.compile("[^)]*\(\?P.*?\)[^(]*")
            groups = splitregexp.findall(_regexp)
            while groups:
                self._regexp.append(re.compile("".join(groups[0:99])))
                del groups[0:99]

        self.skip_empty_lines = self.config.get(self.name, "skip_empty_lines", "boolean", True)

 
        self.delete_extra_spaces = self.config.get(self.name, "delete_extra_spaces", "boolean", True)

        self.skip_first_line = self.config.get(self.name, "skip_first_line", "boolean", False)


        static_fields=self.config.get(self.name, "static_fields")
        if static_fields and type(static_fields) == str:
            self.static_fields = dict()
            for item in static_fields.split(","):
                item = item.strip()
                key, value = item.split("=")
                key = key.strip()
                value = value.strip()
                self.static_fields[key] = value
        else:
            self.static_fields = None

    def __del__(self):
        self.input_file.close()



    def start(self):

        self.line = ""

        self.input_files = copy.copy(self.original_input_files)

        # Process progress
        self.overall_max = len(self.input_files)

        self._next_file()

   
    def _next_file(self):

        try:
            self.input_files
        except:
            self.input_files = copy.copy(self.original_input_files)

        self.overall_current = self.overall_max - (len(self.input_files)-1)

        if self.input_files:
            self.current_file = self.input_files.pop()
        elif self.cyclic:
            self.input_files = copy.copy(self.original_input_files)
            self.current_file = self.input_files.pop()
        else:
            raise StopIteration

        self.log.debug("Opening file (%s)" % self.current_file)
        self.input_file = open(self.current_file, 'r')
        self.step_max = os.path.getsize(self.current_file)
        self.step_current = 0
        self.line_number = 0

        if self.skip_first_line:
            self.log.warning("Skipping first line...")
            self.input_file.readline()
            self.line_number += 1
            

    def next(self, extra_data = None):

        self.line = self.input_file.readline() 
        self.line_number += 1
        while self.skip_empty_lines and self.line == "\n":
            self.log.warning("Skipping empty line. #%s" % self.line_number )
            self.line = self.input_file.readline() 

        if not self.line:
            self.log.debug("End of file (%s)" % self.current_file)
            self._next_file()
            return self.next()

        self.log.debug("Line #%s : %s" % (self.line_number, self.line))
        self.step_current = self.input_file.tell()

        # 100 groups regexp workaround 
        subline = self.line
        result = []
        data = {}
        for subregexp in self._regexp:
            subresult = subregexp.search(self.line) 
            if subresult:
                # Delete matched line part
                subline = subline[subresult.span()[1]:]
                subdata = subresult.groupdict() 
                data.update(subdata)

        if data:
            if self.delete_extra_spaces:
                data = dict(zip(data.keys(), map(lambda x: x.strip(), data.values())))

            if self.static_fields:
                data = dict(data.items() + self.static_fields.items())


            if extra_data and type(extra_data) == dict:
                data.update(extra_data)

            return data
        else:
            self.log.warning("No data found at line #%s" % self.line_number)
            if extra_data and type(extra_data) == dict:
                return extra_data
            return None

