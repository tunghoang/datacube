import yaml
import sys
import os
import uuid
import datacube
import time
from tqdm import tqdm
from .local_config import __BASEPATH
# make dir if not exists
def mkdir(path):
    if not os.path.exists(os.path.dirname(path)): 
        os.makedirs(os.path.dirname(path))

def query(product_name, time_start, time_end):
    dc = datacube.Datacube(app="my_analysis")
    datasets = dc.find_datasets(product=product_name, time=(time_start, time_end))
    return datasets

#==============================
# convert time string for querying
#
# input: time_str (str): e.g. "20181231180000"
#
# output: converted time_str (str): e.g. "2018-12-31 18:00:00"
#==============================
def convert_time_str(time_str):
    year = time_str[: 4]
    month = time_str[4: 6]
    day = time_str[6: 8]
    hour = time_str[8: 10]
    
    time_str = year + '-' + month + '-' + day + ' ' + hour + ':00:00.000Z'
    return time_str
    
#=============================
# generate dataset doc (.yaml)
# 
# input: tif_path (str): path to GeoTIFF image
#
# output: dataset_doc_path (str): path to new dataset doc (.yaml)
#=============================
def gen_dataset_doc(tif_path):
    # #====================================================================================================
    # # set dataset doc path
    # dataset_doc_path = tif_path.replace('.tif', '.dataset.yaml')
    # dataset_doc_path = dataset_doc_path.replace('DATA/DATA', 'DATASET_DOC/DATA')
    # mkdir(dataset_doc_path)
    
    # # export doc to yaml (skip if exists)
    # if os.path.isfile(dataset_doc_path):
    #     return dataset_doc_path
    # #====================================================================================================
            
    #========================
    # check dataset available
    #========================
    
    # get product_name and time in tif_path
    res = tif_path.split('DATA_')[-1].split('/')[0]
    product = tif_path.rsplit('_', 1)[0].split('/')[-1]
    product_name = product + '_' + res
    time_str = tif_path.split('_')[-1].split('.')[0]
    time_str = convert_time_str(time_str)
    
    # check if dataset exist in odc
    # if len(query(product_name, time_str, time_str)) > 0:
    #     print("dataset exists!")
    #     print(tif_path)
    #     return False
    
    # check if tif file exist at tif_path
    if not os.path.isfile(tif_path):
        print("GeoTIFF file not exists!")
        print(tif_path)
        return False
    
    #=====================
    # generate dataset doc
    #=====================
    
    # read template product doc
    yaml_path = f'{__BASEPATH}/TEMPLATE_DOC/dataset.yaml' # path to template doc
    
    # open yaml file as dictionary
    with open(yaml_path, 'r') as file:
        template = yaml.full_load(file)
    
    # write dataset
    id = uuid.uuid4() # random generate uuid (not yet checking duplicated)
    template['id'] = str(id)
    template['product']['name'] = product_name
    resolution = float(res.split('KM')[0]) / 100
    template['grids']['default']['shape'] = [int(i * 0.04 / resolution) for i in [90, 250]]
    template['grids']['default']['transform'] = [resolution, 0.0, 101.0, 
                                                 0.0, -resolution, 21.1, 
                                                 0.0, 0.0, 1.0]
    template['properties']['datetime'] = time_str
    template['measurements']['Precipitation']['path'] = tif_path
    
    #===================
    # export dataset doc
    #===================
    
    # set dataset doc path
    dataset_doc_path = tif_path.replace('.tif', '.dataset.yaml')
    dataset_doc_path = dataset_doc_path.replace('DATA/DATA', 'DATASET_DOC/DATA')
    mkdir(dataset_doc_path)
    
    # export doc to yaml (skip if exists)
    if not os.path.isfile(dataset_doc_path):
        with open(dataset_doc_path, 'w') as file:
            template = yaml.dump(template, file, sort_keys=False)
    
    return dataset_doc_path
    
#=====================
# add dataset to ODC
# 
# input: tif_path (str): path to GeoTIFF image
#
# output: dataset_doc_path (str): path to new dataset doc (.yaml)
#=====================
def add_dataset(path):
    time_start = time.time()
    #=============================================
    # generate datasets corresponding to tif files
    #=============================================
    list_yaml = [] # list of all dataset_doc generated
    
    # process if path is a file
    if os.path.isfile(path):
        print("Adding 1 file.")
        response = gen_dataset_doc(path)
        if not response:
            print("ERROR:", path)
        else:
            list_yaml.append(response)
    
    # process if path is a directory
    elif os.path.isdir(path):
        list_file = [path + '/' + file for path, dirs, files in os.walk(path) for file in files]
        list_file.sort()
        print("Adding", len(list_file), "files.")
        
        for src_path in tqdm(list_file):
            response = gen_dataset_doc(src_path)
            if not response:
                print("ERROR:", src_path)
            else:
                list_yaml.append(response)     
                
    #===================================
    # run command adding datasets to ODC
    #===================================
    print("Add datasets to ODC")
    
    # split list yaml into chunks of size 1000
    size = 1000
    for i in range(0, len(list_yaml), size):
        command = "datacube dataset add " + " ".join(list_yaml[i: i + size])
        os.system(command)
        print('Add', i, min(i + size, len(list_yaml)))
    print("Completed!")
    
    time_stop = time.time()
    print("Time executed:", time_stop - time_start)


if __name__ == '__main__':
    data_folder = f'{__BASEPATH}/DATA/DATA_10KM_daily/Integrated'
    add_dataset(data_folder)
