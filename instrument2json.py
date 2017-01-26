from ctapipe.io.hessio import hessio_event_source
import astropy
import argparse
import numpy as np
import pickle
import gzip

parser = argparse.ArgumentParser(description='Write instrument module from eventio'
                                             'file to json.')
parser.add_argument('input_path', metavar='I', type=str, help='path to eventio file')
parser.add_argument('output_path', metavar='O', type=str, help='path to pickled file')


def serialize_dict_with_units(dct):
    d = {}
    for k, v in dct.items():
        if isinstance(v, float) and np.isnan(v):
            d[str(k)] = 'NaN'
        elif isinstance(v, astropy.units.Quantity):
            s = v.value
            if isinstance(s, np.ndarray):
                s = s.tolist()
            d[str(k)] = {'__value__': s, '__unit__': v.unit.name}
        elif isinstance(v, np.ndarray):
            d[str(k)] = v.tolist()
        elif isinstance(v, dict):
            d[str(k)] = serialize_dict_with_units(v)
        else:
            d[str(k)] = v
    return d


if __name__ == '__main__':
    args = parser.parse_args()
    source = hessio_event_source(args.input_path,  max_events=2)
    instrument = next(source).inst

    print('pickling')
    with gzip.open(args.output_path, 'wb') as f:
        pickle.dump(instrument, f, pickle.HIGHEST_PROTOCOL)

    print('trying to unpickle')
    with gzip.open(args.output_path, 'rb') as f:
        data = pickle.load(f)

    print('success')
