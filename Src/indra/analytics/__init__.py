import sys
from datacube import Datacube
import odc.geo.xr
from odc.geo.geom import Geometry
import numpy as np
from db.extra import extra_get_feature
def aggregate(fn, time_range, gid, level, product='deepModel_10KM_daily'):
    feature = extra_get_feature(level, gid)
    print(level, gid)
    with Datacube() as odc:
        _ds = odc.load(product, measurements=["Precipitation"], time=time_range)
        g = Geometry(feature[0])
        ds = _ds.odc.crop(poly=g, apply_mask=True, all_touched=True)
        print(ds)
        groupbyObj = ds.groupby('time')
        result = ( (
            str(np.datetime_as_string(g[0], unit='D')),
            fn(np.fromiter( ( x for x in g[1].Precipitation.data.flatten() if x > float('-inf') ), dtype=ds.Precipitation.data.dtype )) 
        ) for g in groupbyObj )
        return result

def aggregate1(fn, time_range, product='deepModel_10KM_daily'):
    with Datacube() as odc:
        ds = odc.load(product, measurements=["Precipitation"], time=time_range)
        groupbyObj = ds.groupby('time')
        result = ( (
            str(np.datetime_as_string(g[0], unit='D')),
            fn(np.fromiter( ( x for x in g[1].Precipitation.data.flatten() if x > float('-inf') ), dtype=ds.Precipitation.data.dtype )) 
        ) for g in groupbyObj )
        return result
def do_test(gid, level, time_range, product='deepModel_10KM_daily'):
    feature = extra_get_feature(level, gid)
    with Datacube() as odc:
        ds = odc.load(product, measurements=["Precipitation"], time=time_range)
        return (feature, ds)
