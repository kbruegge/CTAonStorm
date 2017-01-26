from itertools import cycle

from streamparse import Spout
import time
from ctapipe.io.hessio import hessio_event_source
import copy
import numpy as np
import os


file_path = '/home/kbruegge/gamma_test.simtel.gz'


class EventSpout(Spout):
    outputs = ['event']

    def initialize(self, stormconf, context):
        source = hessio_event_source(file_path, max_events=7)
        event_list = list(copy.deepcopy(e) for e in source)
        events = []
        for e in event_list:
            d = {
                 'event_id': np.asscalar(e.dl0.event_id),
                 'run_id': e.dl0.run_id,
                 'data': {},
                 }

            for tel_id, t in e.dl0.tel.items():
                d['data'][str(tel_id)] = {'adc_sums': t.adc_sums[0].tolist()}

            events.append(d)
        self.event_generator = cycle(events)

    def next_tuple(self):
        time.sleep(1.0)
        event = next(self.event_generator)
        # print('Emitting thing!')
        self.emit([event])

#
# class WordSpout(Spout):
#     outputs = ['word']
#
#     def initialize(self, stormconf, context):
#         self.words = cycle(['dog', 'cat', 'zebra', 'elephant'])
#
#     def next_tuple(self):
#         word = next(self.words)
#         self.emit([word])
