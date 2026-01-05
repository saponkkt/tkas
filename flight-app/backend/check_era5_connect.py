#!/usr/bin/env python3
import os
import glob
import sys

print('=== ERA5 connectivity check ===')
CDSURL = os.getenv('CDSAPI_URL') or 'https://cds.climate.copernicus.eu/api'
CDSKEY = os.getenv('CDSAPI_KEY') or None
if not CDSKEY:
    _uid = os.getenv('CDSAPI_UID')
    _secret = os.getenv('CDSAPI_API_KEY')
    if _uid and _secret:
        CDSKEY = f"{_uid}:{_secret}"

print('CDSAPI_URL =', CDSURL)
print('CDSAPI_KEY present =', bool(CDSKEY))

# try import cdsapi and construct client
try:
    import cdsapi
    print('cdsapi import: OK')
    try:
        if CDSKEY:
            c = cdsapi.Client(url=CDSURL, key=CDSKEY)
        else:
            c = cdsapi.Client()
        print('cdsapi.Client(): OK')
    except Exception as e:
        print('cdsapi.Client() failed:', repr(e))
except Exception as e:
    print('cdsapi import failed:', repr(e))

# quick HTTP reachability check
try:
    import requests
    try:
        r = requests.get(CDSURL, timeout=10)
        print('HTTP GET', CDSURL, '=>', r.status_code)
    except Exception as e:
        print('HTTP GET failed:', repr(e))
except Exception as e:
    print('requests not available:', repr(e))

# check local ERA5 NetCDF files
print('\nChecking for local era5_*.nc files in current directory:')
files = glob.glob('era5_*.nc')
print('found files:', files)
if files:
    try:
        import xarray as xr
        for f in files[:5]:
            try:
                ds = xr.open_dataset(f)
                print(f"Opened {f}: vars={list(ds.variables.keys())[:10]}")
                ds.close()
            except Exception as e:
                print(f"Failed to open {f}: {repr(e)}")
    except Exception as e:
        print('xarray open failed or xarray not available:', repr(e))

print('\nDone')
