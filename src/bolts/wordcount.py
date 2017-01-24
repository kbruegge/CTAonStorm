# import os
# from collections import Counter
from ctapipe.io.hessio import hessio_event_source
from streamparse import Bolt
from ctapipe.image.cleaning import tailcuts_clean
from ctapipe.io import CameraGeometry
from ctapipe.image.hillas import hillas_parameters, HillasParameterizationError, MomentParameters
from ctapipe.reco.FitGammaHillas import FitGammaHillas
import numpy as np

from astropy import units as u
import astropy
import simplejson

file_path = '/Users/kbruegge/Development/stormcta/_resources/gamma_test.simtel.gz'


def decode_unit(dct):
    if '__unit__' in dct:
        return dct['__value__']*astropy.units.Unit(dct['__unit__'])


def unit_serializer(obj):
    if isinstance(obj, astropy.units.Quantity):
        return {'__value__': obj.value, '__unit__': obj.unit.name}
    else:
        return simplejson.JSONEncoder.default(obj)


class EmptyBolt(Bolt):

    outputs = ['nada', ]

    def process(self, tup):
        self.logger.info('---'*20)
        self.logger.info(tup)
        self.logger.info('---'*20)
        self.emit(['de nada senior'])


class RecoBolt(Bolt):

    outputs = ['reco']

    def initialize(self, conf, ctx):
        source = hessio_event_source(file_path,  max_events=7)
        self.instrument = next(source).inst
        self.fitter = FitGammaHillas()

    def process(self, tup):
        self.logger.info('recieved tuple')
        self.logger.info(tup)
        hillas_dict = tup.values.hillas
        tel_phi = tup.values.tel_phi
        tel_theta = tup.values.tel_theta
        # hillas_dict = forMomentParameters._make(hillas_dict)
        self.logger.info('calling reco')
        r = self.reco(hillas_dict, tel_phi, tel_theta)
        self.logger.info('emitting reco results')
        self.emit([r])

    def reco(self, hillas_dict, tel_phi, tel_theta):
        self.logger.info('Startign reco')
        fit_result = self.fitter.predict(hillas_dict, self.instrument, tel_phi, tel_theta)
        self.logger.info('Finished reco')
        return fit_result


class HillasBolt(Bolt):

    outputs = ['hillas', 'tel_phi', 'tel_theta']

    def initialize(self, conf, ctx):
        self.total_events = 0
        source = hessio_event_source(file_path,  max_events=7)
        self.instrument = next(source).inst

    def process(self, tup):
        event = tup.values[0]
        self.total_events += 1

        if self.total_events % 25 == 0:
            self.logger.info("counted [{:,}] events [pid={}]".format(self.total_events,
                                                                     self.pid))
        hillas_dict, tel_phi, tel_theta = self.hillas(event)
        self.logger.info('emitting hillas data')
        self.emit([hillas_dict, tel_phi, tel_theta])

    def hillas(self, event):
        hillas_dict = {}
        tel_phi = {}
        tel_theta = {}

        for tel_id in event['data']:
            tel_phi[tel_id] = 0.*u.deg
            tel_theta[tel_id] = 20.*u.deg

            pmt_signal = np.array(event['data'][tel_id]['adc_sums'])
            pix_x = self.instrument.pixel_pos[int(tel_id)][0]
            pix_y = self.instrument.pixel_pos[int(tel_id)][1]
            foc = self.instrument.optical_foclen[int(tel_id)]
            cam_geom = CameraGeometry.guess(pix_x, pix_y, foc)
            # self.logger.info('calling tailcuts')
            mask = tailcuts_clean(cam_geom, pmt_signal, 1,
                                  picture_thresh=10., boundary_thresh=5.)
            pmt_signal[mask == 0] = 0

            try:
                moments = hillas_parameters(cam_geom.pix_x,
                                            cam_geom.pix_y,
                                            pmt_signal)
                # remove dem units bro
                m = [t.value if isinstance(t, astropy.units.Quantity) else t for t in moments]
                hillas_dict[tel_id] = MomentParameters._make(m)
            except HillasParameterizationError as e:
                print(e)

        return hillas_dict, [t.value for t in tel_phi.values()], [t.value for t in tel_theta.values()]
