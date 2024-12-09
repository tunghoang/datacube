import yaml
import os

# make dir if not exists
def mkdir(path):
    if not os.path.exists(os.path.dirname(path)): 
        os.makedirs(os.path.dirname(path))

#=======================================================
# generate product doc (.yaml)
#
# input:
# - product_name (str): GSMaP, AWS, Radar, ...
# - resolution (float): 0.04, 0.1
# - time_interval(str): hourly, daily
#
# output:
# - path to product doc (str)
#=======================================================
def add_product(product, resolution, time_interval):
    
    # read template product doc
    yaml_path = r'/home/odc/Workspace/nfs/TEMPLATE_DOC/product.yaml' # path to template doc
    
    # open yaml file as dictionary
    with open(yaml_path, 'r') as file:
        template = yaml.full_load(file)
    
    # change variable in template
    resolution_km = str(int(resolution / 0.01)) + 'KM'
    product_name = product + '_' + resolution_km + '_' + time_interval
    template['name'] = product_name
    template['description'] = product_name
    template['metadata']['product']['name'] = product_name
    template['storage']['resolution']['latitude'] = - resolution
    template['storage']['resolution']['longitude'] = resolution
    template['measurements'][0]['name'] = product
    template['measurements'][0]['units'] = 'unit'
    
    # create yaml output path and make dirs
    product_doc_path = r'/home/odc/Workspace/nfs/PRODUCT_DOC/DATA_' + resolution_km + '_' + time_interval + '/' + product_name + '.product.yaml'
    mkdir(product_doc_path)
    
    # export yaml to file
    with open(product_doc_path, 'w') as file:
        template = yaml.dump(template, file, sort_keys=False)

    # use command to add product to odc
    os.system('datacube product update --allow-unsafe ' + product_doc_path)
    # return path to product doc
    return product_doc_path

if __name__ == '__main__':
    
    list_product = [
        'CAPE',
        'EWSS',
        'IE',
        'ISOR',
        'KX',
        'PEV',
        'R250',
        'R500',
        'R850',
        'SLHF',
        'SLOR',
        'SSHF',
        'TCLW',
        'TCW',
        'TCWV',
        'U250',
        'U850',
        'V250',
        'V850',
    ]
    for product in list_product:
        # add_product(product, 0.04, 'hourly')
        # add_product(product, 0.1, 'hourly')
        add_product(product, 0.1, 'daily')