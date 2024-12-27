from datacube import Datacube
from .utils import *
if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('--product')
    parser.add_argument('--resolution')
    parser.add_argument('--frequency')
    parser.add_argument('--from-time')
    parser.add_argument('--to-time')
    args = parser.parse_args()

    if all([args.product, args.resolution, args.frequency, args.from_time, args.to_time]):
        with Datacube() as odc:
            datasets = search_datasets(odc, f"{args.product}_{args.resolution}KM_{args.frequency}", from_time=args.from_time, to_time=args.to_time)
            print(datasets)
    else:
        parser.print_help()
