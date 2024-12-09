from fastapi import HTTPException
from datacube import Datacube
from datacube.model import Dataset
from datetime import datetime
import re
import hashlib
import traceback
from constants import SALT
def serialize(ds: Dataset, idx: int):
    matchObj = re.search(r'(.*)_([0-9]+KM)_([a-z]+)$', ds.product.name)
    product = matchObj.group(1)
    resolution = matchObj.group(2)
    frequency = matchObj.group(3)
    return dict(key=idx, id=ds.id, isAvailable=ds.is_active, time=ds.time[0], time1=ds.time[1], name=product, resolution=resolution, frequency=frequency)
def search_datasets(odc: Datacube, product_name, from_time=None, to_time=None, limit=None):
    try:
        datasets = odc.find_datasets_lazy(product=product_name, time=(from_time, to_time), limit=limit)
        dss = [serialize(ds, idx) for idx,ds in enumerate(datasets)]
        dss.sort(key=lambda o: o['time'], reverse=False)
        return dss
    except ValueError as e:
        traceback.print_exc()
        print("EXEPTION:", e)
        raise HTTPException(status_code=404, detail=f"{e}")
def hash_password(p):
    m = hashlib.sha256()
    m.update((p + SALT).encode('utf-8'))
    password_hash = m.digest().hex()
    return password_hash
