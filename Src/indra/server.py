import sys
sys.path.append('/data/lib')
sys.path.append('./indra')
import os
import jwt
import pathlib
import aiofiles
from datetime import datetime

from fastapi import FastAPI, HTTPException,UploadFile, File, Form, Query, Request, Response
from fastapi.responses import FileResponse
from fastapi.middleware.wsgi import WSGIMiddleware

from datacube import Datacube
from .models.Product import Product
from .models.LoginData import LoginData
from .models.UserData import UserData
from .models.DeleteUserData import DeleteUserData
from .add_product import add_product
from .add_dataset import add_dataset
from .del_dataset import del_datasets
from .utils import *
from .local_config import __BASEPATH
from typing import List
from urllib.parse import unquote

from .db import db_get_datetime_limits
from .db.extra import extra_authenticate, extra_list_users, extra_new_user, extra_delete_users

from .constants import SALT, JWT_ALGORITHM, PUBLIC_GET_ROUTES, PUBLIC_POST_ROUTES

from terracotta.server.app import app as terracottaApp
app = FastAPI()
router = FastAPI()

def __filename(apath):
    filename = os.path.basename(apath)
    fname, _ = os.path.splitext(filename)
    return fname

@app.get('/hello')
def hello1():
    return {"message": "Top Hello world1"}

@router.get('/')
def hello():
    return {"message": "router Hello world"}


@router.get('/hello/{param1}/{i}/{s}')
def hello2(param1:str="", i:int = 10, s:str=""):
    return {"message": f"Hello world2: {param1}-{i}-{s}"}
@router.get('/products')
def get_products():
    odc = Datacube()
    products = odc.list_products()
    odc.close()
    return products.drop('default_crs', axis=1).to_dict('records')

@router.get('/measurements')
def get_measurements():
    print("GET measurements")
    odc = Datacube()
    measurementDF = odc.list_measurements()
    measurementDF = measurementDF.reset_index()
    print(measurementDF)
    print(measurementDF.columns)
    odc.close()
    return measurementDF[['product', 'measurement', 'name']].to_dict('records')

@router.post('/products')
def new_product(product: Product):
    product_doc_path = add_product(product.name, product.resolution/100, product.frequency)
    return { "product_doc_path": product_doc_path }

@router.get('/datasets/{product}/{resolution}/{frequency}')
def get_product(product:str = "", resolution:str='10', frequency:str = "daily", from_time:str|None = None, to_time:str | None = None):
    #return {"message": f"datasets: {product}-{resolution}-{frequency}"}
    if from_time is None or to_time is None:
        raise HTTPException(status_code=400, detail="'from_time' and 'to_time' cannot be null")
    print("--- datasets ---")
    with Datacube() as odc:
        datasets = search_datasets(odc, f"{product}_{resolution}KM_{frequency}", from_time=from_time, to_time=to_time)
    return datasets

@router.get('/datasets/download/{id_}')
async def download_dataset(id_:str):
    print(id_)
    with Datacube() as odc:
        dataset = odc.index.datasets.get(id_)
        if dataset is None:
            raise HTTPException(status_code=400, detail="No datasets found")
        file_loc = str(dataset.local_path).replace('DATASET_DOC','DATA').replace('dataset.yaml', 'tif')
        print(file_loc)
        return FileResponse(file_loc)

@router.get('/datasets/download/{product}/{resolution}/{frequency}/{time_str}')
async def get_dataset(product:str = "", resolution:str='10', frequency:str = "daily", time_str:str = ""):
    time_str_ = unquote(time_str)
    time_str_ = '2019-09-09T00:00:00'
    from_time = time_str_
    to_time = time_str_
    with Datacube() as odc:
        datasets = odc.find_datasets(product=f"{product}_{resolution}KM_{frequency}", time=(from_time, to_time), limit=1)
        if len(datasets) == 0:
            raise HTTPException(status_code=400, detail="No datasets found")
        dataset = datasets[0]
        file_loc = str(dataset.local_path).replace('DATASET_DOC','DATA').replace('dataset.yaml', 'tif')
        return FileResponse(file_loc)

@router.get('/datasets/time_limits/{product}/{resolution}/{frequency}')
async def get_dataset_limits(product:str = '', resolution:str='10', frequency:str='daily'):
    productStr = f'{product}_{resolution}KM_{frequency}'
    limits = db_get_datetime_limits(productStr)
    return limits

@router.middleware("http")
async def authorize(request: Request, call_next):
    print(request.headers.get('Authorization'))
    if request.method == 'GET':
        for p in PUBLIC_GET_ROUTES:
            if str(request.url.path).startswith(p):
                return await call_next(request)
    if request.method == 'POST':
        for p in PUBLIC_POST_ROUTES:
            if str(request.url.path).startswith(p):
                return await call_next(request)
    token = request.headers.get('Authorization')
    if token is not None:
        token = token.replace("Bearer ", "")
        try:
            jwt.decode(token, key=SALT, algorithms=JWT_ALGORITHM, options={"verify_signature": True})
            return await call_next(request)
        except jwt.exceptions.InvalidTokenError as e:
            print("Invalid token", e, token)

    return Response(status_code=401)

@router.post('/datasets')
async def post_datasets(product:str = Form(), resolution:str=Form(), frequency:str=Form(), time:datetime=Form(), img: UploadFile=File()):
    print(product, resolution, frequency, time, img)
    sResolution = resolution
    sFrequency = frequency
    productName = f"{product}_{sResolution}_{sFrequency}"
    month = str(time.month).zfill(2)
    day = str(time.day).zfill(2)
    hour = str(time.hour).zfill(2)
    output_path = f"{__BASEPATH}/DATA/DATA_{sResolution}KM_{sFrequency}/{product}/{time.year}/{month}/{day}/{product}_{time.year}{month}{day}{hour}0000.tif"
    print(time)
    print(output_path)
    pathlib.Path(os.path.dirname(output_path)).mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(output_path, 'wb') as out_file:
        while content := await img.read(4096):  # async read chunk
            await out_file.write(content)  # async write chunk
    add_dataset(output_path)
    return {"success": True}

@router.delete('/datasets')
async def delete_datasets(request: Request):
    payload = await request.json()
    print('delete datasets', payload)
    print('ids', payload['ids'])
    
    with Datacube() as odc:
        datasets = [odc.index.datasets.get(id_) for id_ in payload['ids']]
        ids = [str(dataset.id) for dataset in datasets]
        yamls = [dataset.local_uri.replace('file://', '') for dataset in datasets]
        tifs = [path.replace('dataset.yaml', 'tif').replace('DATASET_DOC', 'DATA') for path in yamls]
        del_datasets(ids, yamls, tifs)
        
    return {"success": True, "ids": payload['ids']}


@router.post('/users/login')
async def authenticate(loginData: LoginData):
    password_hash = hash_password(loginData.p)
    userRecord, code = extra_authenticate(loginData.u, password_hash)
    print(userRecord)
    if code == -1:
        raise HTTPException(status_code=400, detail="User not found")
    elif code == -2:
        raise HTTPException(status_code=400, detail="Wrong password")
    elif code == 0:
        encoded_key = jwt.encode(userRecord, SALT, algorithm=JWT_ALGORITHM)
        return {"access_token": encoded_key}
    raise HTTPException(status_code=500, detail="Unexpected situation")

@router.get('/users')
async def get_users(limit: int=10, offset: int=0):
    users = extra_list_users(limit, offset)
    return users

@router.post('/users')
async def new_users(udata: UserData):
    try:
        password_hash = hash_password(udata.password)
        extra_new_user(udata.email, udata.fullname, password_hash)
        return {'success': True, 'message': 'success'}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"{e}")

@router.delete('/users')
async def delete_users(delUserData: DeleteUserData):
    try: 
        userIds = delUserData.userIds
        extra_delete_users(userIds)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")

import terracotta as tc
tc.update_settings(DRIVER_PATH='mysql://root:dbpassword@mysql/terracotta')

app.mount('/api', router)
app.mount('/', WSGIMiddleware(terracottaApp))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(f"indra.{os.path.basename(__filename(__file__))}:app", host="0.0.0.0", port=8888, log_level="info", reload=True, workers=1, loop='uvloop')
