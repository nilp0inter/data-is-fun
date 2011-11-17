#!/usr/bin/env python
"""Transformadores para text2db.

Un transformador es un objeto que obtiene como entrada una cadena y
puede generar a partir de ella la sentencia, en el lenguaje destino,
para representar el dato de entrada.

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

class transformer:
    def __init__(self, input_file):

        self.config = Config(input_file)

        self.name = self.config.get("transformer", "name", "string", "")
        self.regexp = self.config.get("transformer", "regexp", "string", "")
        self.formatter = self.config.get("transformer", "formatter", "string", "")
        self.output_format = self.config.get("transformer", "output_format", "string", "")

        self._regexp = re.compile(self.regexp)

    def match(self, s):
        """Match string with transformer.
        """

        match = self._regexp.match(s)
        if match:
            return True
        else:
            return False

    def transform(self, s):
        """Return string transformation.
        """

        match =  self._regexp.match(s)
        if match:
            return self.formatter % match.groupdict()
        else:
            Exception(ValueError, 'Can\'t transform (%s) to type %s using %s
            transformer' % (s, self.output_format, self.name))
