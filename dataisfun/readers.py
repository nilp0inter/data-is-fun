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

from dataisfun.util import progressbar

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
        # Process progress
        self.overall_max = -1
        self.overall_current = 0
        self.step_max = -1
        self.step_current = 0
        self.last_overall_current = -1

    def finish(self):
        pass

    def __iter__(self):
        return self

    def update_progress(self, screen_percent):

        if self.overall_current != self.last_overall_current:
            # Reader step change, update progress
            self.last_overall_current = self.overall_current
            return self.get_progress(screen_percent)
        else:
            # Reader step update
            try:
                if self.overall_max == -1 or self.cyclic:
                    pass
                else:
                    self.progress.update(self.step_current) 
                return self.progress
            except AttributeError:
                return self.get_progress(screen_percent)
            except AssertionError:
                print self.step_current, self.step_max
                return self.get_progress(screen_percent)

    def get_progress(self, screen_percent):
        if self.overall_max == -1 or self.cyclic:
            widgets = [ ' %s : N/A ' % self.name ]
            self.progress = progressbar.ProgressBar(widgets=widgets, term_width=screen_percent, maxval=1000, fd=None)
            self.progress.start()
        else:
            widgets = [ ' %s (%s/%s):' % (self.name, self.overall_current,
            self.overall_max), progressbar.Percentage(),
            progressbar.Bar(marker='*', left='[', right=']'), progressbar.ETA() ]
            self.progress = progressbar.ProgressBar(widgets=widgets, term_width=screen_percent, maxval=self.step_max, fd=None)
            self.progress.start()
            self.progress.update(self.step_current)



        return self.progress

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

