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
import logging

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

class Writer(object):
    """
        Clase padre de los escritores. 
    """

    def __init__(self, config, name):
        self.name = name
        self.type = self.__class__.__name__.lower()
        self.log = logging.getLogger('writer.%s' % self.name)
        self.config = config
        self.log.debug("Writer (%s) starting..." % self.name)

    def start(self):
        pass

    def finish(self):
        pass

## Writer Skeleton
#class writername(Writer):
#    """
#        writername info
#    """
#
#    def __init__(self, config, name):
#        self._library = __import__('library')
#       
#        super(readername, self).__init__(config, name)
#
#    def start(self):
#        pass
#
#    def add_data(self, data):
#        pass
#
#    def finish(self):
#        pass


