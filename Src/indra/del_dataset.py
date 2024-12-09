import yaml
import os
import uuid
import datacube
import time
from tqdm import tqdm
from typing import List

# make dir if not exists
def mkdir(path):
    if not os.path.exists(os.path.dirname(path)): 
        os.makedirs(os.path.dirname(path))

def query(product_name, time_start, time_end):
    dc = datacube.Datacube(app="my_analysis")
    datasets = dc.find_datasets(product=product_name, time=(time_start, time_end))
    return datasets

def delete_empty_folders(root):
    deleted = set()
    for current_dir, subdirs, files in os.walk(root, topdown=False):
        still_has_subdirs = False
        for subdir in subdirs:
            if os.path.join(current_dir, subdir) not in deleted:
                still_has_subdirs = True
                break
    
        if not any(files) and not still_has_subdirs:
            os.rmdir(current_dir)
            deleted.add(current_dir)

    return deleted

#=================================================================================
# remove dataset in time range
# 
# input:
# - product_name (str): GSMaP_4KM_hourly
# - time_start, time_end (str): '2020-01-01'
# 
# output:
# - notification
#=================================================================================
def del_dataset(product_name, time_start = '2010-01-01', time_end = '2030-01-01'):
    t_start = time.time()
    list_dataset = query(product_name, time_start, time_end)
    # terminate if no dataset found
    if len(list_dataset) == 0:
        return
    
    #======================
    # prepare list id, file
    #======================
    # extract id and yaml path
    list_id = [str(dataset.id) for dataset in list_dataset]
    list_yaml = [dataset.local_uri.replace('file://', '') for dataset in list_dataset]
    
    # get list tif from list yaml
    list_data = [path.replace('dataset.yaml', 'tif').replace('DATASET_DOC', 'DATA') for path in list_yaml]
    
    #========================
    # delete dataset from ODC
    #======================== 
    # split list id into chunks of size 1000
    size = 1000
    for i in range(0, len(list_id), size):
        join_id = ' '.join(list_id[i: i + size])
        
        command = "datacube dataset archive " + join_id
        os.system(command)
        
        command = "datacube dataset purge " + join_id
        os.system(command)
        
        print('Remove', i, min(i + size, len(list_id)))
    
    #=============
    # delete files
    #=============
    # move data to trash
    for src_path in list_data:
        dst_path = src_path.replace('/DATA/', '/Trash/DATA/')
        mkdir(dst_path)
        os.rename(src_path, dst_path)
        
    # delete yaml file
    for path in list_yaml:
        os.remove(path)
    
    t_stop = time.time()
    print("Time executed:", t_stop - t_start)

def del_datasets(ids: List[str], yamls: List[str], data_files: List[str]):
    join_ids = ' '.join(ids)
    
    command = "datacube dataset archive " + join_ids
    os.system(command)
    
    command = "datacube dataset purge " + join_ids
    os.system(command)
    
    paths = ' '.join(f'"{y}"' for y in yamls)
    command = 'rm -fr ' + paths
    os.system(command)

    paths = ' '.join(f'"{df}"' for df in data_files)
    command = 'rm -fr ' + paths
    os.system(command)
    
    print('Remove', join_ids)

if __name__ == '__main__':
    # for product in ['Radar', 'GSMaP', 'IMERG_E', 'IMERG_L', 'IMERG_F', 'CCS', 'FY4A']:
        # del_dataset(product + '_4KM_hourly')
        # del_dataset(product + '_10KM_hourly')
        # del_dataset(product + '_10KM_daily')
    del_dataset('Radar_4KM_hourly', '2019-01-01', '2019-01-01')
