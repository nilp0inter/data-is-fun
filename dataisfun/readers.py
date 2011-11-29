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


import logging
import sys

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

class Reader(object):
    """
        Clase padre de los lectores. 
    """

    def __init__(self, config, name):
        self.name = name
        self.type = self.__class__.__name__.lower()
        self.log = logging.getLogger('reader.%s' % self.name)
        self.config = config
        self.log.debug("Reader (%s) starting..." % self.name)

        # Values for all reader
        self.cyclic = self.config.get(self.name, "cyclic", "boolean", False)

    def start(self):
        pass

    def finish(self):
        pass

    def __iter__(self):
        return self

## Reader Skeleton
#class readername(Reader):
#    """
#        readername info
#    """
#
#    def __init__(self, config, name, input_files):
#        self._library = __import__('library')
#       
#        super(readername, self).__init__(config, name)
#
#    def start(self):
#        pass
#
#    def next(self, extra_data = None):
#        pass
#
#    def finish(self):
#        pass


class mysql(Reader):
    """
        Lector de mysql.
        Se le especifica la query (puede utilizarse sustitucion de variables).
        Mientras la query no cambie se recorren los resultados obtenidos hasta 
        su final.
    """

    def __init__(self, config, name, input_files):
        self._mysqldb = __import__('MySQLdb.cursors')
       
        super(mysql, self).__init__(config, name)

        self.hostname = self.config.get(self.name, "hostname")
        self.database = self.config.get(self.name, "database")
        self.username = self.config.get(self.name, "username")
        self.password = self.config.get(self.name, "password")
        self.query = self.config.get(self.name, "query")
        self.requery = self.config.get(self.name, "requery", "boolean", False)
        self.last_query = None

    def do_query(self, sql):
        self.log.debug("Executing query: %s" % sql)
        self.cursor.execute(sql)

    def start(self):
        self.db = self._mysqldb.connect(host=self.hostname, user=self.username, passwd=self.password, db=self.database, cursorclass=self._mysqldb.cursors.SSDictCursor, conv={})

        self.cursor = self.db.cursor()

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
            raise StopIteration

        elif extra_data:
            extra_data.update(data)
            data=extra_data

        return data
            
    def finish(self):
        self.cursor.close()
        self.db.close()
        self.last_query = None

##class udf(Reader):
##    """
##        User defined functions or values.
##        Inspired in transformers.
##
##    """
##    def __init__(self, config, input_files):
##
##        functions = self.config.c.items(self.name)
##
##        if functions:
##            try:
##                # Get and compile lambda functions (groups{}) => str)
##                pre_format_pairs = dict(filter(lambda x: x[0].startswith('preformat_'), functions))
##                self.f_preformat = {}
##                for name, f in pre_format_pairs.iteritems():
##                    try:
##                        self.f_preformat[name[10:]] = eval(compile(f, '<string>','eval'), {}, {})
##                    except Exception, e:
##                        self.log.error("Error compiling preformat function %s at %s(%s) [%s]" % (name, self.name, self.config_file, e))
##                        raise
## 
    
class command(Reader):
    """
        Lector command.
            Ejecuta un comando y devuelve su resultado.
            Soporta reemplazo de argumentos.
    """

    def __init__(self, config, name, input_files):
        self._subprocess = __import__('subprocess')
       
        super(command, self).__init__(config, name)

        commands = self.config.c.items(self.name)
        commands = dict(filter(lambda x: x[0].startswith('exec_'), commands))

        self.strip = self.config.get(self.name, "strip", "boolean", True)

        self.command = {}
        for name, exestr in commands.iteritems():
            self.command[name[5:]] = exestr

    def next(self, extra_data = None):

        data = {}
        if extra_data:
            data.update(extra_data)

        for name, exestr in self.command.iteritems():
            if extra_data:
                exestr = exestr % extra_data

            exestr = filter(lambda x: x, exestr.split(' '))

            if self.strip:
                data[name] = self._subprocess.Popen(exestr, stdout=self._subprocess.PIPE).communicate()[0].rstrip('\r\n')
            else:
                data[name] = self._subprocess.Popen(exestr, stdout=self._subprocess.PIPE).communicate()[0]

        return data

class csv(Reader):
    """
        Lector de csv.
    """

    def __init__(self, config, name, input_files):
        self._csv = __import__('csv')
        self._copy = __import__('copy')
       
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
        self.input_files = self._copy.copy(self.original_input_files)
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
            self.input_files = self._copy.copy(self.original_input_files)

        if self.input_files:
            self.current_file = self.input_files.pop()
        elif self.cyclic:
            self.input_files = self._copy.copy(self.original_input_files)
            self.current_file = self.input_files.pop()
        else:
            raise StopIteration

        if self.input_file:
            self.input_file.close()

        self.log.debug("Opening file (%s)" % self.current_file)
        self.input_file = open(self.current_file, 'rb')
      
        if not self.dialect:
            dialect = self._csv.Sniffer().sniff(self.input_file.read(2048))
            self.input_file.seek(0)
        else:
            dialect = self.dialect

        self.input_csv = self._csv.DictReader(self.input_file, fieldnames=self.fieldnames, restkey=self.restkey, restval=self.restval, dialect=dialect)
        self.line_number = 0

class regexp(Reader):
    """
        Clase reader (iterable). Recibe un fichero o nombre
         de fichero y una expresion regular. Parsea cada linea
         y devuelve el diccionario de valores parseados.
    """

    def __init__(self, config, name, input_files):
        self._re = __import__('re')
        self._copy = __import__('copy')
       
        super(regexp, self).__init__(config, name)

        _regexp = self.config.get(self.name, "regexp")

        if type(input_files) != list:
            self.original_input_files = [ input_files ]
        else:
            self.original_input_files = input_files

        try:
            self._regexp = [self._re.compile(_regexp)]
            self.long_regexp = False
        except:
            # Python no soporta mas de 100 grupos nominales.
            # Como "probablemente" estemos utilizando una expresion regular para capturar
            # campos de ancho fijo, generaremos N expresiones regulares con un maximo 
            # de 100 campos nominales cada una
            
            self.long_regexp = True
            self._regexp = []
            splitregexp = self._re.compile("[^)]*\(\?P.*?\)[^(]*")
            groups = splitregexp.findall(_regexp)
            while groups:
                self._regexp.append(self._re.compile("".join(groups[0:99])))
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
        self.line_number = 0

        self.input_files = self._copy.copy(self.original_input_files)
        self._next_file()
    
    def _next_file(self):

        try:
            self.input_files
        except:
            self.input_files = self._copy.copy(self.original_input_files)

        if self.input_files:
            self.current_file = self.input_files.pop()
        elif self.cyclic:
            self.input_files = self._copy.copy(self.original_input_files)
            self.current_file = self.input_files.pop()
        else:
            raise StopIteration

        self.log.debug("Opening file (%s)" % self.current_file)
        self.input_file = open(self.current_file, 'r')

        if self.skip_first_line:
            self.log.warning("Skipping first line...")
            self.input_file.readline()

            
    def next(self, extra_data = None):

        self.line = self.input_file.readline() 
        self.line_number += 1
        while self.skip_empty_lines and self.line == "\n":
            self.log.warning("Skipping empty line. #%s" % self.line_number )
            self.line = self.input_file.readline() 
            self.line_number += 1
            
        if not self.line:
            self.log.debug("End of file (%s)" % self.current_file)
            self._next_file()
            return self.next()

        self.log.debug("Line #%s : %s" % (self.line_number, self.line))

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

