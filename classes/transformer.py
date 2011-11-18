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

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinez.es"
__status__ = "Production"

from config import Config
from topological_sort import robust_topological_sort as ts

class transformer:
    def __init__(self, input_file):

        self.config = Config(input_file)

        self.name = self.config.get("transformer", "name", "string", "")
        self.regexp = self.config.get("transformer", "regexp", "string", "")

        try:
            functions = self.config.c.items('functions')
        except:
            functions = False
        
        if functions:
#            try:
            # Get and compile pre-format lambda functions
            pre_format_pairs = dict(filter(lambda x: x[0].startswith('pre_format_'), functions))
            self.pre_format = {}
            for name, f in pre_format_pairs.iteritems():
                self.pre_format[name[11:]] = eval(compile(f, '<string>','eval'))

            # Get and compile matcher lambda functions
            matcher_pairs = dict(filter(lambda x: x[0].startswith('matcher_'), functions))
            self.matchers = {}
            for name, f in matcher_pairs.iteritems():
                self.matchers[name[8:]] = eval(compile(f, '<string>','eval'))

            # Get and compile post-format lambda function (formated_str, values) => final_str
            post_format = self.config.get("functions", "post_format", "string", "")
            if post_format:
                self.post_format = eval(compile(post_format, '<string>','eval'))
            else:
                self.post_format = False
        else:
            self.post_format = False
            self.matchers = False
            self.pre_format = False

        self.formatter = self.config.get("transformer", "formatter", "string", "")

        self.output_format = self.config.get("transformer", "output_format", "string", "").upper()

        self._regexp = re.compile(self.regexp)

    def match(self, s):
        """Match string with transformer.
        """

        match = self._regexp.match(s)
        if match:
            if self.matchers:
                groups = match.groupdict()
                # Return True if all matcher functions return True
                return all(map(lambda x: self.matchers[x](groups[x]) if self.matchers.has_key(x) and groups[x]!=None else True, groups.keys()))
            return True
        else:
            return False

    def __repr__(self):
        return "%s(%s)" % (self.output_format, self.name)

    def transform(self, s):
        """Return string transformation.
        
           No test matching! 
        """
      
        match =  self._regexp.match(s)
        if match:
            groups = match.groupdict()

            if self.pre_format:
                # Execute preformat functions
                for name, value in groups.iteritems():
                    if name in self.pre_format.keys():
                        groups[name] = self.pre_format[name](groups)

            formatted = self.formatter % groups 

            # Useless functionality?
            if self.post_format:
                return self.post_format(formatted, groups)

            return formatted
        else:
            Exception(ValueError, 'Can\'t transform (%s) to type %s using %s transformer' % (s, self.output_format, self.name))

class transform:
    def __init__(self, transformers_path, force_output_format=False):
        self.transformers = [] 
        if force_output_format:
            self.force_output_format = force_output_format.upper()
        else:
            self.force_output_format = False

        for infile in glob.glob( os.path.join(transformers_path, '*.tf') ):
            t = transformer(infile)
            if self.force_output_format:
                if t.output_format != self.force_output_format:
                    del t
                    continue

            self.transformers.append(t)

    def get_transformers(self, s):
        """Return matching transformers
        """
        self.transformers = [ x for x in self.transformers if x.match(s) ]
        
        return self.transformers

if __name__ == '__main__':
    t = transform('transformers/')
    value = "2011-03-27"
    for t in t.get_transformers(value):
        print t,'=>', t.transform(value)

