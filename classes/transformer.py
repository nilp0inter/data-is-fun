#!/usr/bin/env python
"""Transformadores para text2db.

Un transformador es un objeto que obtiene como entrada una cadena y
puede generar, a partir de ella, la sentencia en el lenguaje destino
para representar el dato de forma nativa.

"""


import re
import os
import glob
import logging
from MySQLdb import escape_string as escape

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinez.es"
__status__ = "Production"


from config import Config
from topological_sort import robust_topological_sort as ts


class tvalue:
    def __init__(self, original, transformed, datatype, size=None, typesize=None):
        self.original = original
        self.transformed = transformed
        self.datatype = datatype
        self.size = size
        self.typesize = typesize 

    def __repr__(self):
        return "%s" % self.transformed

class transformer:
    def __init__(self, config_file, nullable=False):

        self.config_file = config_file
        self.nullable = nullable

        self.config = Config(self.config_file)

        self.name = self.config.get("transformer", "name", "string", "")
        self.regexp = self.config.get("transformer", "regexp", "string", "")
        self.formatter = self.config.get("transformer", "formatter", "string", "")
        self.output_type = self.config.get("transformer", "output_type", "string", "").upper()
        self.type_format = self.config.get("transformer", "type_format", "string", "")
        self.compatible_writers = map(lambda x: x.strip(), self.config.get("transformer", "compatible_writers", "string", "").split(","))
        self.last_type_size = []

        if not self.compatible_writers:
            self.compatible_writers= []
        self.loaded = False

    def load(self):
        self._regexp = re.compile(self.regexp)

        try:
            functions = self.config.c.items('functions')
        except:
            functions = False
        
        if functions:
            # Get and compile pre-format lambda functions (groups{}) => str)
            pre_format_pairs = dict(filter(lambda x: x[0].startswith('preformat_'), functions))
            self.pre_format = {}
            for name, f in pre_format_pairs.iteritems():
                self.pre_format[name[10:]] = eval(compile(f, '<string>','eval'))

            # Get and compile matcher lambda functions (groups{}) => True || False
            matcher_pairs = dict(filter(lambda x: x[0].startswith('matcher_'), functions))
            self.matchers = {}
            for name, f in matcher_pairs.iteritems():
                self.matchers[name[8:]] = eval(compile(f, '<string>','eval'))

            # Get and compile post-format lambda function (formated_str, values) => final_str
            post_format = self.config.get("functions", "postformat", "string", "")
            if post_format:
                self.post_format = eval(compile(post_format, '<string>','eval'))
            else:
                self.post_format = False

            # Get and compile size lambda function (groups{}) => size || None
            size = self.config.get("functions", "size", "string", "")
            if size:
                self.size = eval(compile(size, '<string>','eval'))
            else:
                self.size = False

            # Get and compile typesize lambda function (groups{}) => list || None
            typesize = self.config.get("functions", "typesize", "string", "")
            if typesize:
                self.typesize = eval(compile(typesize, '<string>','eval'))
            else:
                self.typesize = False

        else:
            self.post_format = False
            self.matchers = False
            self.pre_format = False
            self.size = False
            self.typesize = False

        self.loaded = True

    def _match(self, s):
        """Match string with transformer and return groups.
        """

        assert self.loaded

        match = self._regexp.match(s)
        if match:
            groups = match.groupdict()
            if self.matchers:
                # Return True if all matcher functions return True
                if all(map(lambda x: self.matchers[x](groups) if self.matchers.has_key(x) and groups[x]!=None else True, groups.keys())):
                    return groups
            else:
                return groups
        return False

    def match(self, s):
        """Match string with transformer.
        """

        m = self._match(s)
        if m != False:
            m.update({ '_last_type_size': self.last_type_size})
            if self.size:
                size = self.size(m)
            else:
                size = None
            # Calc typesize
            if self.typesize:
                self.last_type_size = self.typesize(m)

            return tvalue(s, None, self.type_format, size, self.last_type_size)
        else:
            if self.nullable:
                return tvalue(s, None, None)
            else:
                return False

    def __repr__(self):
        return "%s %s" % (self.type_format, ("NULL" if self.nullable else "NOT NULL"))

    def transform(self, s):
        """Return string transformation.
        """
      
        groups = self._match(s)
        if groups != False:
            try:
                if self.pre_format:
                    # Execute preformat functions
                    for name, value in groups.iteritems():
                        if name in self.pre_format.keys():
                            groups[name] = self.pre_format[name](groups)

                formatted = self.formatter % dict(map(lambda x: (x[0], escape(x[1]) if type(x[1]) == str else x[1]), groups.iteritems()))

                # Useless functionality?
                if self.post_format:
                    return self.post_format(formatted, groups)

                groups.update({ '_last_type_size': self.last_type_size})
                # Calc size
                if self.size:
                    size = self.size(groups)
                else:
                    size = None

                # Calc typesize
                if self.typesize:
                    self.last_type_size = self.typesize(groups)

                return tvalue(s, formatted, self.type_format, size, self.last_type_size)

            except Exception, e:
                if self.nullable:
                    return tvalue(s, None, None) # NULL
                else:
                    raise RuntimeError('Error in transformation using %s (%s)' % (self.name, e))
        elif self.nullable:
            return tvalue(s, None, None) # NULL
        else:
            raise TypeError('Can\'t transform (%s) to type %s using %s transformer' % (s, self.type_format, self.name))

class transform_factory:
    def __init__(self, transformers_path, force_output_type = False,
            force_output_writer = False, nullable = False, type_size = None):

        self.transformers = {} 
        self.nullable = nullable 

        if force_output_type:
            if type(force_output_type) is not list:
                force_output_type = [ force_output_type ]
            self.force_output_type = map(lambda x: x.upper(), force_output_type)
        else:
            self.force_output_type = False

        self.force_output_writer = force_output_writer
        for infile in glob.glob( os.path.join(transformers_path, '*.tf') ):
            t = transformer(infile, nullable = nullable)
            if self.force_output_type:
                if t.output_type not in self.force_output_type:
                    del t
                    continue

            if self.force_output_writer:
                if self.force_output_writer not in t.compatible_writers:
                    del t
                    continue

            # Load regex and functions if not discarded
            t.load()
            self.transformers[t] = {'accumulated_size' : None, 
                                    'nulls' : 0,
                                    'accumulated_typesize' : type_size}

    def set_nullable(self):
        if not self.nullable:
            for t in self.transformers.keys():
                t.nullable = True
            self.nullable = True
        

    def _match(self, t, s):
        """Call transformer match and feed statistics
        """
        m = t.match(s)
        if m != False:
            if m.size:
                if self.transformers[t]['accumulated_size']:
                    self.transformers[t]['accumulated_size'] += m.size
                else:
                    self.transformers[t]['accumulated_size'] = m.size
            if m.typesize:
                if self.transformers[t]['accumulated_typesize']:
                    self.transformers[t]['accumulated_typesize'] = max((m.typesize,self.transformers[t]['accumulated_typesize']))
                else:
                    self.transformers[t]['accumulated_typesize'] = m.typesize

            if m.datatype == None:
                self.transformers[t]['nulls'] += 1
        return m

    def get_transformer_definition(self, t, stats):
        if stats['accumulated_typesize']:
            try:
                return str(t) % (','.join(map(lambda x: str(x), stats['accumulated_typesize'])))
            except TypeError:
                return str(t)
        else:
            return str(t)

    def get_transformers(self, s):
        """Return matching transformers
        """
        return [ x for x in self.transformers.keys() if self._match(x, s) ]
       
    def adjust(self, s):
        """Adjust transformer list to match new data
        """

        map(lambda x: self.transformers.__delitem__(x), set(self.transformers.keys()).difference(set(self.get_transformers(s))))

        ## DEBUG
        #for t,stats in self.transformers.iteritems():
        #    print self.get_transformer_definition(t, stats)
    
    def _get_best_transformer(self):
        # Get the transformer with lower nulls and accumulated_size
        s = sorted(self.transformers.iteritems(), key = lambda x: (x[1]['nulls'], x[1]['accumulated_size']) )    
        return s[0][0]

    def get_best_definition(self):
        # Get the transformer with lower nulls and accumulated_size
        t = self._get_best_transformer()
        return self.get_transformer_definition(t, self.transformers[t])

    def transform(self, value):
        t = self._get_best_transformer()
        return t.transform(value)
        
if __name__ == '__main__':
    t = transform_factory('/home/segl/text2db/classes/transformers/', force_output_writer = 'mysql' )
    t.adjust("70")
    print t.get_best_definition()
    t.adjust("712")
    print t.get_best_definition()
    t.adjust("723")
    print t.get_best_definition()
    t.adjust("734")
    print t.get_best_definition()
    t.adjust("7")
    print t.get_best_definition()
