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
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

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
    def __init__(self, config_file, nullable=False, match_count = 0):

        self.nullable = nullable
        self.match_count = match_count
        self.last_type_size = []
        self.loaded = False
        self.config_file = config_file

        self.log = logging.getLogger('transformer')

        # Config options
        self.config = Config(self.config_file)

        self.name = self.config.get("transformer", "name", "string", "")
        self.regexp = self.config.get("transformer", "regexp", "string", "")
        self.formatter = self.config.get("transformer", "formatter", "string", "")
        self.output_type = self.config.get("transformer", "output_type", "string", "").upper()
        self.type_format = self.config.get("transformer", "type_format", "string", "")
        self.compatible_writers = map(lambda x: x.strip(), self.config.get("transformer", "compatible_writers", "string", "").split(","))

        if not self.compatible_writers:
            self.compatible_writers = []

    def load(self):
        self._regexp = re.compile(self.regexp)

        try:
            functions = self.config.c.items('functions')
        except:
            functions = False
        
        if functions:
            try:
                # Get and compile pre-format lambda functions (groups{}) => str)
                pre_format_pairs = dict(filter(lambda x: x[0].startswith('preformat_'), functions))
                self.f_preformat = {}
                for name, f in pre_format_pairs.iteritems():
                    try:
                        self.f_preformat[name[10:]] = eval(compile(f, '<string>','eval'), {}, {})
                    except Exception, e:
                        self.log.error("Error compiling preformat function %s at %s(%s) [%s]" % (name, self.name, self.config_file, e))
                        raise
                         

                # Get and compile matcher lambda functions (groups{}) => True || False
                matcher_pairs = dict(filter(lambda x: x[0].startswith('matcher_'), functions))
                self.f_matchers = {}
                for name, f in matcher_pairs.iteritems():
                    try:
                        self.f_matchers[name[8:]] = eval(compile(f, '<string>','eval'), {}, {})
                    except Exception, e:
                        self.log.error("Error compiling matcher function %s at %s(%s) [%s]" % (name, self.name, self.config_file, e))
                        raise

                # Get and compile post-format lambda function (formated_str, values) => final_str
                post_format = self.config.get("functions", "postformat", "string", "")
                if post_format:
                    try:
                        self.f_postformat = eval(compile(post_format, '<string>','eval'), {}, {})
                    except Exception, e:
                        self.log.error("Error compiling postformat function at %s(%s) [%s]" % (self.name, self.config_file, e))
                        raise
                else:
                    self.f_postformat = False

                # Get and compile size lambda function (groups{}) => size || None
                size = self.config.get("functions", "size", "string", "")
                if size:
                    try:
                        self.f_size = eval(compile(size, '<string>','eval'), {}, {})
                    except Exception, e:
                        self.log.error("Error compiling size function at %s(%s) [%s]" % (self.name, self.config_file, e))
                        raise
                else:
                    self.f_size = False

                # Get and compile typesize lambda function (groups{}) => list || None
                typesize = self.config.get("functions", "typesize", "string", "")
                if typesize:
                    try:
                        self.f_typesize = eval(compile(typesize, '<string>','eval'), {}, {})
                    except Exception, e:
                        self.log.error("Error compiling typesize function at %s(%s) [%s]" % (self.name, self.config_file, e))
                        raise
                else:
                    self.f_typesize = False
            except:
                self.log.warning("Disabling transformer %s" % self.name)
                return

        else:
            self.f_postformat = False
            self.f_matchers = False
            self.f_preformat = False
            self.f_size = False
            self.f_typesize = False

        self.loaded = True

    def _get_udfs_input(self, groups):
        r = {}
        r.update(groups)
        r.update({ '_last_type_size': self.last_type_size,\
                '_match_count': self.match_count})

        return r

    def _match(self, s):
        """Match string with transformer and return groups.
        """

        assert self.loaded

        m = self._regexp.match(s)
        if m:
            groups = m.groupdict()
            if self.f_matchers:
                # Return True if all matcher functions return True
                if all(map(lambda x: self.f_matchers[x](groups) if self.f_matchers.has_key(x) and groups[x]!=None else True, groups.keys())):
                    return groups
            else:
                return groups
        return False

    def match(self, s):
        """Match string with transformer.
        """

        m = self._match(s)
        if m != False:
            if self.f_size:
                size = self.f_size(self._get_udfs_input(m))
            else:
                size = None
            # Calc typesize
            if self.f_typesize:
                self.last_type_size = self.f_typesize(m)

            self.match_count += 1
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
                if self.f_preformat:
                    # Execute preformat functions
                    for name, value in groups.iteritems():
                        if name in self.f_preformat.keys():
                            groups[name] = self.f_preformat[name](groups)

                formatted = self.formatter % dict(map(lambda x: (x[0], escape(x[1]) if type(x[1]) == str else x[1]), groups.iteritems()))

                # Useless functionality?
                if self.f_postformat:
                    return self.f_postformat(formatted, groups)

                groups = self._get_udfs_input(groups)
                # Calc size
                if self.f_size:
                    size = self.f_size(groups)
                else:
                    size = None

                # Calc typesize
                if self.f_typesize:
                    self.last_type_size = self.f_typesize(groups)

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
        self.virgin = True

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
            if not t.loaded:
                del t
                continue

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
        self.virgin = False
        if s == None:
            self.nullable = True
            return

        map(lambda x: self.transformers.__delitem__(x), set(self.transformers.keys()).difference(set(self.get_transformers(s))))

        ## DEBUG
        #for t,stats in self.transformers.iteritems():
        #    print self.get_transformer_definition(t, stats)
    
    def _get_best_transformer(self):
        # Get the transformer with lower nulls and accumulated_size
        s = sorted(self.transformers.iteritems(), key = lambda x: (x[1]['nulls'], x[1]['accumulated_size']) )    
        try:
            return s[0][0]
        except:
            return None

    def get_best_definition(self):
        # Get the transformer with lower nulls and accumulated_size
        t = self._get_best_transformer()
        if not t:
            return None
        return self.get_transformer_definition(t, self.transformers[t])

    def transform(self, value):
        if self.virgin:
            self.adjust(value)
           
        if value == None:
            if not self.nullable:
                raise ValueError('Trying to transform None value with a non nullable data type')
            else:
                return None

        t = self._get_best_transformer()
        if not t:
            return '"%s"' % value
        else:
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
