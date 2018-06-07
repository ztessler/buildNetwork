# vim: fileencoding=UTF-8
# vim: filetype=python

### TO RUN:
# DELTA=[Mekong] STNdomain=[Global] STNres=[06min] scons -i
# (need -i to ignore the core dump in netBuild and keep going. output is fine)

import os
import sys
import lib

SetOption('max_drift', 1)

env = Environment(ENV = {'PATH' : os.environ['PATH'],
                         'GDAL_DATA': os.environ['GDAL_DATA'],
                         })
env.Decider('MD5-timestamp')

work = 'work'
output = 'output'

delta = os.environ.get('DELTA', 'Mekong')
STNdomain = os.environ.get('STNdomain', 'Asia')
STNres = os.environ.get('STNres', '06min')

#initial_network = '/asrc/RGISarchive2/{domain}/Network/HydroSTN30/{res}/Static/{domain}_Network_HydroSTN30_{res}_Static.gdbn.gz'.format(domain=STNdomain, res=STNres)
initial_network = '/asrc/ecr/balazs/Projects/2018/2018-05_HydroSTN30v100/RGISlocal/{domain}/Network/HydroSTN30ext/{res}/Static/{domain}_Network_HydroSTN30ext_{res}_Static.gdbn.gz'.format(domain=STNdomain, res=STNres)


env.Command(
        source = initial_network,
        target = os.path.join(work, '{domain}_{res}.1.gdbc'.format(domain=STNdomain, res=STNres)),
        action='netCells2Grid -f BasinID -t BasinID -u BasinID -d Global $SOURCE $TARGET')
env.Command(
        source = os.path.join(work, '{domain}_{res}.1.gdbc'.format(domain=STNdomain, res=STNres)),
        target = os.path.join(work, '{domain}_{res}.2.gdbc'.format(domain=STNdomain, res=STNres)),
        action='grdRenameLayers -r 1 XXXX $SOURCE $TARGET')
env.Command(
        source = os.path.join(work, '{domain}_{res}.2.gdbc'.format(domain=STNdomain, res=STNres)),
        target = os.path.join(work, '{domain}_{res}.nc'.format(domain=STNdomain, res=STNres)),
        action='rgis2netcdf $SOURCE $TARGET')
env.Command(
        source = os.path.join(work, '{domain}_{res}.nc'.format(domain=STNdomain, res=STNres)),
        target = os.path.join(work, '{domain}_{res}.tif'.format(domain=STNdomain, res=STNres)),
        action = lib.georef_nc_to_tif)

# get delta extents
# group all the little shapes together (geo.group_delta_shps)
env.Command(
        source = '/Users/ecr/ztessler/data/deltas_LCLUC/maps/global_map_shp/global_map.shp',
        target = os.path.join(work, '{}.json'.format(delta)),
        action = lib.group_delta_shps,
        delta = delta)

# find all contributing basins (geo.contributing basins)
# write those to file, or print out
env.Command(
        source = [os.path.join(work, '{}.json'.format(delta)),
                  os.path.join(work, '{}_{}.tif'.format(STNdomain, STNres))],
        target = os.path.join(work, '{}_basins_{}_{}.txt'.format(delta, STNres, STNdomain)),
        action = lib.delta_basins,
        delta = delta)

# extract basins from network (gives a table)
env.Command(
        source = [os.path.join(work, '{}_basins_{}_{}.txt'.format(delta, STNres, STNdomain)),
                  initial_network],
        target = os.path.join(work, '{}_Network_{}_{}.gdbt'.format(delta, STNres, STNdomain)),
        action = 'xargs -a ${SOURCES[0]} -n 1 -i echo BasinID != {} "&&" | tr "\\n" " " | xargs -i tblDeleteRec -a DBCells -c \"{} BasinID != -8888\" ${SOURCES[1]} $TARGET')

# build, trim, rebuild network
env.Command(
        source = os.path.join(work, '{}_Network_{}_{}.gdbt'.format(delta, STNres, STNdomain)),
        target = os.path.join(work, '{}_Network_{}_{}.build.gdbn'.format(delta, STNres, STNdomain)),
        action = 'netBuild -t {delta}_Network_{res} -u "Network" -d {delta} $SOURCE $TARGET'.format(delta=delta, res=STNres))

# trim network - renumbers so shouldn't matter what was originating domain
env.Command(
        source = os.path.join(work, '{}_Network_{}_{}.build.gdbn'.format(delta, STNres, STNdomain)),
        target = os.path.join(work, '{}_Network_{}.trim.gdbn'.format(delta, STNres)),
        action = 'netTrim -t {delta}_Network_{res} -u "Network" -d {delta} $SOURCE $TARGET'.format(delta=delta, res=STNres))

# rebuild
env.Command(
        source = os.path.join(work, '{}_Network_{}.trim.gdbn'.format(delta, STNres)),
        target = os.path.join(output, '{}_Network_{}.gdbn'.format(delta, STNres)),
        action = 'netBuild -t {delta}_Network_{res} -u "Network" -d {delta} $SOURCE $TARGET'.format(delta=delta, res=STNres))
