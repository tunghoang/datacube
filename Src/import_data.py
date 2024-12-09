from indra.add_dataset import add_dataset
import re
from tc_import import tc_load
#skip_converts = [True, False, False]
#skip_converts = [False, False]
skip_converts = [False]
#for idx, loc in enumerate(('/data/DATA/DATA_10KM_daily', '/data/DATA/DATA_10KM_hourly', '/data/DATA/DATA_4KM_hourly')) :
#for idx, loc in enumerate(('/data/DATA/DATA_10KM_hourly', '/data/DATA/DATA_4KM_hourly')) :
for idx, loc in enumerate(('/data/DATA/DATA_4KM_hourly',)) :
    m = re.match('/data/DATA/DATA_([0-9]+)KM_([a-z]+)', loc)
    params = m.groups()
    tc_load(loc, int(params[0]), params[1], skip_convert=skip_converts[idx])
    #add_dataset(loc)

print("Done")
