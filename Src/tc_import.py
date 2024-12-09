import os
import subprocess
from multiprocessing import Process
from glob import glob
import terracotta as tc
import argparse
from terracotta.scripts.optimize_rasters import _optimize_single_raster
from rasterio.enums import Resampling
import re
from pathlib import Path
from typing import Dict, List
from tqdm import trange
from tqdm.contrib import tenumerate

DB_CONN = 'mysql://root:dbpassword@mysql/terracotta'
#DB_CONN = '/data/optimized/tiles.sqlite'

def tc_ingest(dbConnStr, keys: List[str], raster_files: List[Dict]) :
    driver = tc.get_driver(dbConnStr)
    try:
        driver.create(keys)
    except:
        pass

    fwrite = open('/data/error.txt', 'w+')

    with driver.connect():
        # insert metadata for each raster into the database
        print("Ingest rasters ...")
        for idx,raster in tenumerate(raster_files):
            try:
                driver.insert(raster["key_values"], raster["path"])
            except Exception as e:
                print("Error", e)
                fwrite.write(raster['path'] + '\n')
        fwrite.close()

def tc_prepare(filePath, resolution, frequency, procStr, skip_convert=False):
    org = Path(filePath)
    dest = Path(f'/data/optimized/{frequency}_{resolution}km/')
    fname = os.path.basename(filePath)
    if not skip_convert:
        dest.mkdir(parents=True, exist_ok=True)
        if not os.path.exists(f'/data/optimized/{frequency}_{resolution}km/{fname}'):
            _optimize_single_raster(
                org, 
                dest,
                False, Resampling.average, 
                True,
                'DEFLATE', 
                True, procStr)
    m = re.match('([a-zA-Z0-9_]+)_([0-9]+)\.tif', fname)
    if m is None:
        raise Exception('File not match')
    comps = m.groups()
    product = comps[0]
    timestr = comps[1]

    return dict(key_values=dict(
        product=product,
        resolution=resolution,
        frequency=frequency,
        timestr=timestr
    ), path=f'/data/optimized/{frequency}_{resolution}km/{fname}')

def chunks(lst, n):
    l = len(lst)
    size = l // n + 1
    count = 0
    for i in trange(0, len(lst), size):
        yield (count, lst[i:i + size])
        count += 1

def tc_load_single(dbConnStr, paths, resolution, frequency, skip_convert, idx):
    inputFile = f'/data/input.{idx}.path'
    print("start thread", idx)
    fin = open(inputFile, 'w')
    fin.write('\n'.join(paths))
    fin.close()
    os.system(f'python tc_import.py --resolution {resolution} --frequency {frequency} --index {idx}')
    print('end thread', idx)

def do_load_single(dbConnStr, resolution, frequency, skip_convert, idx, chunk):
    logFile = f'/data/error.{idx}.txt'
    inputFile = f'/data/input.{idx}.path'
    ferror = open(logFile, 'w+')
    cnt = 0
    for fpath in chunk:
        fpath = fpath.strip()
        if fpath:
            raster = tc_prepare(fpath, resolution, frequency, f'{idx+1}/{idx+1}', skip_convert=skip_convert)
            try: 
                driver = tc.get_driver(dbConnStr)
                with driver.connect():
                    driver.insert(raster['key_values'], raster['path'])
            except Exception as e:
                ferror.write('{fpath} -> {e}\n')
            cnt += 1
            if cnt % 2 == 0:
                print(f"Process {idx}: {cnt}")
    ferror.close()
    print("end thread", idx)
    
def tc_load(rootPath, resolution, frequency, skip_convert=False):
    driver = tc.get_driver(DB_CONN)
    try:
        driver.create(['product', 'resolution', 'frequency', 'timestr'])
    except:
        pass

    if os.path.isdir(rootPath):
        print('scan dirrrrrr ...')
        to_process = []
        for idx,fpath in tenumerate(glob(f'{rootPath}/**/*.tif', recursive=True)):
            if os.path.basename(fpath) not in ('DEM_BTB_10KM.tif','DEM_BTB_4KM.tif'):
                to_process.append(fpath)

        print('scan dir end', len(to_process))
        do_load_single(DB_CONN, resolution, frequency, skip_convert, 1, to_process)
        '''
        threads = [ Process(
                target=do_load_single, 
                args=(DB_CONN, resolution, frequency, skip_convert, idx, chunk)
            ) for idx,chunk in chunks(to_process, 12) ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        '''
    else:
        raster = tc_prepare(rootPath, resolution, frequency, f'{1}/{1}', skip_convert=skip_convert)
        tc_ingest(DB_CONN, ['product', 'resolution', 'frequency', 'timestr'], [raster])

def tc_load_1(rootPath, resolution, frequency, skip_convert=False):
    to_process = [rootPath]
    if os.path.isdir(rootPath):
        to_process = glob(f'{rootPath}/**/*.tif', recursive=True)

    raster_files = []
    print('converting files ...')
    for idx, fpath in tenumerate(to_process):
        if os.path.isfile(fpath) and os.path.basename(fpath) not in ('DEM_BTB_10KM.tif','DEM_BTB_4KM.tif'):
            raster_files.append(tc_prepare(fpath, resolution, frequency, f'{idx+1}/{len(to_process)}', skip_convert=skip_convert))

    tc_ingest(DB_CONN, ['product', 'resolution', 'frequency', 'timestr'], raster_files)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--resolution', type=int, required=True)
    parser.add_argument('--frequency', required=True)
    parser.add_argument('--file', type=str)
    parser.add_argument('--index', type=int)
    args = parser.parse_args()
    if args.index is not None:
        do_load_single(DB_CONN, args.resolution, args.frequency, False, args.index)
    elif args.file is not None:
        tc_load(args.file, args.resolution, args.frequency, skip_convert=False)
        #tc_load_1(args.file, args.resolution, args.frequency, skip_convert=True)

if __name__ == '__main__':
    main()
