#!/usr/bin/env python
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
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinez.es"
__status__ = "Production"

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
