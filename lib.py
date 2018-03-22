import numpy as np
from collections import OrderedDict
from netCDF4 import Dataset
import rasterio
import geopandas
from affine import Affine
import shapely.geometry as sgeom
from rasterstats import zonal_stats


def georef_nc_to_tif(env, target, source):
    nc = Dataset(str(source[0]))
    var = nc.variables[nc.subject]
    data = var[:].squeeze().data.astype(np.int32)
    nodata = var.missing_value
    lat_bnds = nc.variables['latitude_bnds'][:]
    lon_bnds = nc.variables['longitude_bnds'][:]

    yoff, xoff = data.shape
    sx = np.diff(lon_bnds).mean()
    sy = np.diff(lat_bnds).mean()

    affine = Affine.translation(lon_bnds.min(), lat_bnds.max()) * Affine.scale(sx, -sy)
    with rasterio.open(str(target[0]), 'w',
            driver='GTiff', width=xoff, height=yoff,
            crs={'init':'epsg:4326'}, transform=affine,
            count=1, nodata=nodata, dtype=str(data.dtype)) as dst:
        dst.write(np.flipud(data), 1)

    nc.close()
    return 0


def group_delta_shps(env, target, source):
    deltas = geopandas.GeoDataFrame.from_file(str(source[0]))
    delta = env['delta']
    crs = deltas.crs

    deltas = deltas.groupby('Delta')\
                   .aggregate({
                       'DeltaID': lambda s: s.iloc[0],
                       'geometry': lambda s: sgeom.MultiPolygon(list(s)),
                        })
    delta = geopandas.GeoDataFrame(deltas.loc[delta,:]).T
    delta['Delta'] = delta.index #index lost on saving to file
    delta.crs = crs

    delta.to_file(str(target[0]), driver='GeoJSON')
    return 0


def delta_basins(env, target, source):
    delta = env['delta']
    def ma_unique_values(ma):
        return {int(b) for b in ma[np.logical_not(ma.mask)]}

    stats = zonal_stats(
            str(source[0]), str(source[1]),
            geojson_out=True,
            add_stats={'basins': ma_unique_values})

    data = OrderedDict()
    for s in stats:
        data[s['properties']['Delta']] = sorted(s['properties']['basins'])

    tups = []
    with open(str(target[0]), 'w') as fout:
        for d, basins in data.items():
            fout.write('\n'.join([str(b) for b in basins]) + '\n')

    return 0

