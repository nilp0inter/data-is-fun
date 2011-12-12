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


import os
import sys
import codecs
import logging

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

from dataisfun.writers import Writer 

class file_format(Writer):

    """ Class file_format
        Write files with custom format.

    """

    def __init__(self, config, name):
       
        super(file_format, self).__init__(config, name)

        self.format = self.config.get(self.name, "format", "string")
        if not self.format:
            raise ValueError("The format value is mandatory")

        self.output_location = self.config.get(self.name, "output", "string")
        if not self.output_location:
            raise ValueError("The output value is mandatory")


        self.append = self.config.get(self.name, "append", "boolean", True)
        self.skiperrors = self.config.get(self.name, "skiperrors", "boolean", True)
        self.encoding = self.config.get(self.name, "encoding", "string", "utf-8")


    def start(self):
        pass   

    def add_data(self, data):
        try:
            if self.output_filename != self.output_location % data:
                self.output_file.close()
                self.output_filename = self.output_location % data
                if self.append:
                    self.output_file = codecs.open(self.output_filename, mode='a', encoding=self.encoding)
                else:
                    self.output_file = codecs.open(self.output_filename, mode='w', encoding=self.encoding)
        except (NameError, AttributeError):
            self.output_filename = self.output_location % data
            if self.append:
                self.output_file = codecs.open(self.output_filename, mode='a', encoding=self.encoding)
            else:
                self.output_file = codecs.open(self.output_filename, mode='w', encoding=self.encoding)

        try:
            self.output_file.write(self.format % data)
            self.output_file.write('\n')
        except Exception, e:
            if not self.skiperrors:
                raise
            else:
                self.log.warning("Error writing plain data: %s", e)

    def finish(self):
        try:
            self.output_file.close()
        except:
            pass

            

