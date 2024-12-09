import os
from osgeo import gdal, osr
from tqdm import tqdm
from datetime import datetime, timedelta
import cv2
import numpy as np
from pathlib import Path
from .local_config import __TMPPATH
gdal.DontUseExceptions()
count = 0

def extract_product_datetime(file_name: str):
    # shorten file name
    file_name = file_name.split('.')[0].split('_')[-1]
    return datetime.strptime(file_name, '%Y%m%d%H%M%S')

def generate_product_output(dt: datetime, product: str):
    return dt.strftime(f'%Y/%m/%d/{product}_%Y%m%d%H%M%S.tif')

def mkdir(path):
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
        
def process_resample(product, src_dir, dst_dir, time_str=None):
    src_dir = Path(src_dir)
    dst_dir = os.path.join(Path(dst_dir), f'{product}_BTB_10KM_1h')
    list_file = [(path, file_name) 
                    for path, dirs, files in os.walk(src_dir) 
                    for file_name in files 
                    if os.path.splitext(file_name)[-1] == '.tif'
                    if file_name.rsplit('_', 1)[0] == product]
    list_file.sort()
    for path, src_name in tqdm(list_file):
        dt = extract_product_datetime(src_name)
        src_path = os.path.join(path, src_name)
        dst_path = os.path.join(dst_dir, generate_product_output(dt, product))
        if time_str:
            time = datetime.strptime(time_str, "%Y%m%d")
            if (dt - timedelta(hours=1)).replace(hour=0) != time:
                continue
        
        mkdir(dst_path)
        gdal.Warp(
            dst_path,
            src_path,
            format = 'GTiff',
            dstSRS = "+proj=longlat + ellps=WGS84 + datum=WGS84 + no_defs",
            xRes = 0.1, yRes = 0.1,
            outputBounds = (101, 17.5, 111, 21.1),
            resampleAlg = "average",
            creationOptions = ['COMPRESS=LZW'],
        )
    
    return dst_dir

def export_daily(dst_dir, paths, date, product):
    # Kiểm tra sự đầy đủ của dữ liệu hàng giờ
    if len(paths) < 24:
        return False
    # define meta variables
    driver = gdal.GetDriverByName('GTiff')
    projection = osr.SpatialReference()
    projection.ImportFromEPSG(4326)
    projection = projection.ExportToWkt()
    geotransform = 101, 0.1, 0, 21.1, 0, -0.1
    nodata = - float('inf')
    
    img = np.array([cv2.imread(path, -1) for path in paths])
    img = img.sum(axis=0)
    img[img < 0] = nodata
    dst_path = os.path.join(dst_dir, generate_product_output(date, product))
    mkdir(dst_path)
    
    dst_data = driver.Create(
        dst_path,
        img.shape[1],
        img.shape[0],
        1,
        gdal.GDT_Float32,
        options=['COMPRESS=LZW'],
    )
    
    dst_data.SetProjection(projection)
    dst_data.SetGeoTransform(geotransform)
    dst_data.GetRasterBand(1).WriteArray(img)
    dst_data.GetRasterBand(1).SetNoDataValue(nodata)
    dst_data.FlushCache()
    dst_data = None
    global count
    count = count + 1
    
def process_todaily(product, src_dir, dst_dir, time_str=None):
    src_dir = Path(src_dir)
    dst_dir = os.path.join(Path(dst_dir), f'{product}_BTB_10KM_1d')
    # generate list files in input dir
    list_file = [(path, file_name) 
                    for path, dirs, files in os.walk(src_dir) 
                    for file_name in files 
                    if os.path.splitext(file_name)[-1] == '.tif'
                    if file_name.rsplit('_', 1)[0] == product]
    list_file.sort()
    
    cache = []
    last_date = datetime(2010, 12, 31)
    for path, src_name in tqdm(list_file):
        current_date = extract_product_datetime(src_name) - timedelta(hours=1)
        current_date = current_date.replace(hour=0)
        src_path = os.path.join(path, src_name)
        
        if time_str:
            time = datetime.strptime(time_str, "%Y%m%d")
            if current_date != time:
                continue
            
        if current_date != last_date:
            export_daily(dst_dir, cache, last_date, product)
            cache.clear()
            
        # cập nhật danh sách tạm thời
        cache.append(src_path)
        last_date = current_date
    
    export_daily(dst_dir, cache, last_date, product)


def post_process(product, src_dir, dst_dir, time_str=None):
    path = process_resample(product, src_dir, dst_dir, time_str)
    process_todaily(product, path, dst_dir, time_str)

if __name__ == '__main__':
    product = 'IMERG_E'
    src_dir = f'{__TMPPATH}/Test/Input'
    dst_dir = f'{__TMPPATH}/Test/Output'
    # time_str = '20201018'
    post_process(product, src_dir, dst_dir)
    print(count)
