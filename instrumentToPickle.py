from ctapipe.io.hessio import hessio_event_source
import argparse
import pickle
import gzip

parser = argparse.ArgumentParser(description='Write instrument module from eventio'
                                             'file to json.')
parser.add_argument('input_path', metavar='I', type=str, help='path to eventio file')
parser.add_argument('output_path', metavar='O', type=str, help='path to pickled file')

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
