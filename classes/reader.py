#!/usr/bin/env python
"""Lectores para text2db.

Estas clases reciben interpretan datos desde distintos medios y los transforman
en diccionarios clave valor para enviarlos al core. Deben ser iteradores ya que 
el core los tratara como tal.
"""


import re
import logging
import sys

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinez.es"
__status__ = "Production"


class reader:
    """
        Clase reader (iterable). Recibe un fichero o nombre
         de fichero y una expresion regular. Parsea cada linea
         y devuelve el diccionario de valores parseados.
    """

    def __init__(self, config, input_file):

        regexp = config.get("reader", "regexp")

        self.log = logging.getLogger('main.reader')

        if input_file != file:
            self.input_file = open(input_file, "r")
        else:
            self.input_file = input_file

        try:
            self.regexp = [re.compile(regexp)]
            self.long_regexp = False
        except:
            # Python no soporta mas de 100 grupos nominales.
            # Como "probablemente" estemos utilizando una expresion regular para capturar
            # campos de ancho fijo, generaremos N expresiones regulares con un maximo 
            # de 100 campos nominales cada una
            
            self.long_regexp = True
            self.regexp = []
            splitregexp = re.compile("[^)]*\(\?P.*?\)[^(]*")
            groups = splitregexp.findall(regexp)
            while groups:
                self.regexp.append(re.compile("".join(groups[0:99])))
                del groups[0:99]

        self.skip_empty_lines = config.get("reader", "skip_empty_lines", "boolean", True)
 
        self.delete_extra_spaces = config.get("reader", "delete_extra_spaces", "boolean", True)

        skip_first_line=config.get("reader", "skip_first_line", "boolean", False)
        if skip_first_line:
            self.log.warning("Skipping first line...")
            self.input_file.readline()

        static_fields=config.get("reader", "static_fields")
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

        self.line = ""
        self.line_number = 0

        self.log.debug("File reader started...")

    def __del__(self):
        self.input_file.close()

    def __iter__(self):
        return self

    def next(self):
        self.line = self.input_file.readline() 
        self.line_number += 1
        while self.skip_empty_lines and self.line == "\n":
            self.log.warning("Skipping empty line. #%s" % self.line_number )
            self.line = self.input_file.readline() 
            self.line_number += 1
            
        if not self.line:
            self.log.debug("End of file.")
            raise StopIteration

        self.log.debug("Line #%s : %s" % (self.line_number, self.line))

        # 100 groups regexp workaround 
        subline = self.line
        result = []
        data = {}
        for subregexp in self.regexp:
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

            self.log.debug("Data found: %s" % data)
            return data
        else:
            self.log.warning("No data found at line #%s" % self.line_number)
            return None

