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
"""Traductores para text2db.

Un traductor es un objeto que obtiene como entrada una cadena que 
representa una sentencia, en el lenguaje destino, que genera un tipo
de dato y la transforma a otra sentencia destino con un dato distinto.

Un traductor de datetime -> date generaría lo siguiente:
input: "datetime('2011-04-05')"
output "date(datetime('2011-04-05'))"

"""


import re
import logging

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

from config import Config

class translator:
    def __init__(self, input_file):

        self.config = Config(input_file)

        self.name = self.config.get("translator", "name", "string", "")
        self.formatter = self.config.get("translator", "formatter", "string", "")
        self.input_format = self.config.get("translator", "input_format", "string", "")
        self.output_format = self.config.get("translator", "output_format", "string", "")

        self._regexp = re.compile(self.regexp)

    def translate(self, s):
        """Return string translation.
        """

        return self.formatter % s
