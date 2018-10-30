import numpy as np
import pandas
from collections import OrderedDict
from netCDF4 import Dataset
import rasterio
from rasterio.features import shapes
import fiona
import geopandas
from affine import Affine
import shapely.geometry as sgeom
import shapely.ops as sops
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


def vectorize_joined_basins(source, target, env):
    delta = env['delta']
    with rasterio.open(str(source[0])) as rast:
        data = rast.read(1)
        data[data == rast.nodata] = 0
        data[data != 0] = 1
        affine = rast.transform

    polys = shapes(data, mask=data.astype(rasterio.uint8), connectivity=8, transform=affine)
    geoms = [geom for (geom, val) in polys]
    records = [{'properties': {'BASIN': delta},
                'geometry': geom,
               } for geom in geoms]

    with fiona.open(str(target[0]), 'w',
                    driver='ESRI Shapefile',
                    crs={'init':'epsg:4326'},
                    schema={
                        'geometry': 'Polygon',
                        'properties': {'BASIN': 'str'},
                        }) as vec:
        vec.writerecords(records)

    return 0


def merge_shpfiles(source, target, env):
    with fiona.open(str(source[0])) as inshp:
        driver = inshp.driver
        crs = inshp.crs
        schema = inshp.schema

    with fiona.open(str(target[0]), 'w',
            driver=driver,
            crs=crs,
            schema=schema) as outshp:

        for s in source:
            with fiona.open(str(s)) as inshp:
                for record in inshp:
                    outshp.write(record)

    return 0


def buffer_shpfile(source, target, env):
    buffer_dist = env['buffer_dist']

    with fiona.open(str(source[1])) as coasts:
        lands = [c for c in coasts if c['properties']['area'] > 10000]
        land = [sgeom.shape(record['geometry']) for record in lands]
        land = sgeom.MultiPolygon(land)


    with fiona.open(str(source[0])) as inshp:
        driver = inshp.driver
        crs = inshp.crs
        schema = inshp.schema

        polys = [sgeom.shape(record['geometry']) for record in inshp]
        multi = sgeom.MultiPolygon(polys)
        dissolved = sops.unary_union(multi)
        clipped = dissolved.intersection(land)
        buffered = clipped.buffer(buffer_dist) # deg lat/lon

        record = {'properties': {'BASIN': 'merged'},
                  'geometry': sgeom.mapping(buffered)}

        with fiona.open(str(target[0]), 'w',
                driver=driver,
                crs=crs,
                schema=schema) as outshp:
            outshp.write(record)

    return 0


def mouths_in_ssea(source, target, env):
    mouths = pandas.read_csv(str(source[0]))
    lonmin, lonmax, latmin, latmax = env['bbox']

    ssea = mouths[(mouths['MouthXCoord'] >= lonmin) &
                  (mouths['MouthXCoord'] <= lonmax) &
                  (mouths['MouthYCoord'] >= latmin) &
                  (mouths['MouthYCoord'] <= latmax)]
    ssea.to_csv(str(target[0]), columns=['ID'], index=False, header=False)
    return 0
