from itertools import cycle

from streamparse import Spout

import numpy as np
import os.path as path
import pickle
import gzip

WORKING_DIR = path.dirname(path.dirname(path.realpath(__file__)))


class EventSpout(Spout):
    outputs = ['event']

    def initialize(self, stormconf, context):
        p = path.join(WORKING_DIR, 'bundled_files', 'gammas.pickle.gz')
        with gzip.open(p, 'rb') as f:
            event_list = pickle.load(f)

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
        # time.sleep(0.1)
        event = next(self.event_generator)
        # print('Emitting thing!')
        self.emit([event])
