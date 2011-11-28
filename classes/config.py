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

"""Clase de configuracion basica sobre ficheros de texto.

"""

import logging
import ConfigParser

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"


class Config:
    def __init__(self, config_file):
        self.c = ConfigParser.RawConfigParser()
        self.c.read(config_file)
        self.log = logging.getLogger('main.config')

    def get(self, section, option, option_type="string", default=None):
        """
            Carga una opcion del fichero de configuracion.
            Version safe getopt

        """

        value = default
        try:
            if option_type == "string":
                value = self.c.get(section,option)
            elif option_type == "boolean":
                value = self.c.getboolean(section,option)
            elif option_type == "int":
                value = self.c.getint(section,option)
            elif option_type == "float":
                value = self.c.getfloat(section,option)
        except:
            value = default

        self.log.debug("Setting option %s\\%s = %s" % (section, option, value))
        return value

