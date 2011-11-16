import re
import csv
import sys
import unicodedata
import codecs
import  cStringIO

# Workaround utf8 at csv module
class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self

def elimina_tildes(s):
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))
 


def text2tablename(s, prefix = None):
    """
        Convierte cualquier cadena en un nombre de tabla de Mysql valido eliminando caracteres y 
            truncando a una longitud de 62.
    """

    if prefix:
        maxlength = 62 - (len(prefix)+1)
    else:
        maxlength = 62

    basename = re.sub('_+', '_', \
                re.sub('[^a-z0-9\ ]', '', \
                elimina_tildes(s).lower()).strip().replace(' ', '_'))[0:maxlength]

    if prefix:
        return "%s_%s" % (prefix, basename)
    else:
        return basename

def pic2length(s):
    """
        Obtiene la longitud de un tipo de dato PIC X:
           PIC X     ----> 1
           PIC X(10) ----> 10 
    """
    if s.find('(') != -1:
        return int(s[s.find('(')+1:s.find(')')])
    else:
        return 1


def build_subregexp(fieldname, size):
    if size == 1:
        return "(?P<%s>.)" % fieldname
    else:
        return "(?P<%s>.{%s})" % (fieldname, size)


#f = codecs.open(sys.argv[1], encoding='ISO-8859-1', mode='r')
f = open(sys.argv[1], mode='r')
# Skip first
f.readline()
reader = UnicodeReader(f, dialect=csv.excel, encoding="ISO-8859-1", delimiter=';', quotechar="\"")

fields = []
subregexps = []
for row in reader:
    basefield = text2tablename(row[0])
    fieldname = basefield
    count = 2 
    while fieldname in fields:
        fieldname = "%s_%s" % (basefield, count)
        count += 1

    fields.append(fieldname)
    subregexps.append(build_subregexp(fieldname, pic2length(row[1])))

print "".join(subregexps)
