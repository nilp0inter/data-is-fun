import inspect

import multiprocessing
from time import sleep
from Queue import Queue as QueueClass

class Module(multiprocessing.Process):
    def __init__(self):
        super(Module, self).__init__()
        self._in = {}
        self._out = {}
        self.max_size = 2048
        # Get all functions starting with _input_XXXXX and create an input
        # queue
        self._define_inputs()

    def _define_inputs(self):
        for name, f in inspect.getmembers(self, inspect.ismethod):
            if name.startswith("_input_"):
                self._new_input(name.replace('_input_','',1), f, self.max_size)

    def _new_input(self, name, f, maxsize):
        self._in[name] = (multiprocessing.Queue(maxsize), f)

    def _new_output(self, name):
        self._out[name] = None

    def _set_output(self, name, q):
        if not isinstance(q, multiprocessing.queues.Queue):
            print q.__class__
            raise TypeError('Output must be a Queue object')
        self._out[name] = q

    def get_input(self, name):
        return self._in[name][0]

    def _send(self, name, data):
        if self._out[name] != None:
            self._out[name].put(data)
        else:
            pass

    def insert_data(self, name, data):
        """ Manual insert data into input """
        self._in[name][0].put(data)

    def connect(self, others_input, my_output_name):
        self._set_output(my_output_name, others_input)
        
    def run(self):
        while 1:
            for name, (q, f) in self._in.iteritems():
                if not q.empty():
                    f(q.get())

class SampleWriter(Module):
    def __init__(self):
        super(SampleWriter, self).__init__()

    def _input_defaultinput(self, data):
        print data

class SampleReader(Module):
    def __init__(self):
        super(SampleReader, self).__init__()
        self._new_output('defaultoutput')

    def run(self):
        while 1:
            self._send('defaultoutput', 1) 

if __name__ == '__main__':
    a = SampleReader()
    b = SampleWriter()

    a.connect(b.get_input('defaultinput'), 'defaultoutput')

    a.start()
    b.start()

    try:
        while 1:
            sleep(1)
    except KeyboardInterrupt:
        a.terminate()
        b.terminate()
