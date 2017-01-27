from ctapipe.io.hessio import hessio_event_source
import argparse
import pickle
import gzip
import copy

parser = argparse.ArgumentParser(description='Write MC to pickle.')
parser.add_argument('input_path', metavar='I', type=str, help='path to eventio file')
parser.add_argument('output_path', metavar='O', type=str, help='path to pickled file')
parser.add_argument('events', metavar='N', type=int, help='number of events to pickle')

if __name__ == '__main__':
    args = parser.parse_args()
    source = hessio_event_source(args.input_path,  max_events=args.events)
    event_list = list(copy.deepcopy(e) for e in source)

    print('pickling')
    with gzip.open(args.output_path, 'wb') as f:
        pickle.dump(event_list, f, pickle.HIGHEST_PROTOCOL)

    print('trying to unpickle')
    with gzip.open(args.output_path, 'rb') as f:
        data = pickle.load(f)

    print('success')
