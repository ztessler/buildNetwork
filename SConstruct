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

topwork = 'work'
topoutput = 'output'

#deltas = ['Mekong', 'Chao Phraya']
deltas = ['Mekong', 'Chao Phraya', 'Irrawaddy', 'Ganges', 'Brahmani', 'Mahanadi', 'Godavari', 'Krishna']

STNdomain = os.environ.get('STNdomain', 'Asia')
STNres = os.environ.get('STNres', '06min')

#initial_network = '/asrc/RGISarchive2/{domain}/Network/HydroSTN30/{res}/Static/{domain}_Network_HydroSTN30_{res}_Static.gdbn.gz'.format(domain=STNdomain, res=STNres)
initial_network = '/asrc/ecr/balazs/Projects/2018/2018-05_HydroSTN30v100/RGISlocal/{domain}/Network/HydroSTN30ext/{res}/Static/{domain}_Network_HydroSTN30ext_{res}_Static.gdbn.gz'.format(domain=STNdomain, res=STNres)

shpfiles = []

for delta in deltas:
    dname = delta.replace(' ','_')
    work = os.path.join(topwork, dname)
    output = os.path.join(topoutput, dname)

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
            target = os.path.join(work, '{}.json'.format(dname)),
            action = lib.group_delta_shps,
            delta = delta)

    # find all contributing basins (geo.contributing basins)
    # write those to file, or print out
    env.Command(
            source = [os.path.join(work, '{}.json'.format(dname)),
                      os.path.join(work, '{}_{}.tif'.format(STNdomain, STNres))],
            target = os.path.join(work, '{}_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
            action = lib.delta_basins,
            delta = delta)

    # extract basins from network (gives a table)
    env.Command(
            source = [os.path.join(work, '{}_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
                      initial_network],
            target = os.path.join(work, '{}_Network_{}_{}.gdbt'.format(dname, STNres, STNdomain)),
            action = 'xargs -a ${SOURCES[0]} -n 1 -i echo BasinID != {} "&&" | tr "\\n" " " | xargs -i tblDeleteRec -a DBCells -c \"{} BasinID != -8888\" ${SOURCES[1]} $TARGET')

    # build, trim, rebuild network
    env.Command(
            source = os.path.join(work, '{}_Network_{}_{}.gdbt'.format(dname, STNres, STNdomain)),
            target = os.path.join(work, '{}_Network_{}_{}.build.gdbn'.format(dname, STNres, STNdomain)),
            action = 'netBuild -t {dname}_Network_{res} -u "Network" -d {dname} $SOURCE $TARGET'.format(dname=dname, res=STNres))

    # trim network - renumbers so shouldn't matter what was originating domain
    env.Command(
            source = os.path.join(work, '{}_Network_{}_{}.build.gdbn'.format(dname, STNres, STNdomain)),
            target = os.path.join(work, '{}_Network_{}.trim.gdbn'.format(dname, STNres)),
            action = 'netTrim -t {dname}_Network_{res} -u "Network" -d {dname} $SOURCE $TARGET'.format(dname=dname, res=STNres))

    # rebuild
    env.Command(
            source = os.path.join(work, '{}_Network_{}.trim.gdbn'.format(dname, STNres)),
            target = os.path.join(output, '{}_Network_{}.gdbn'.format(dname, STNres)),
            action = 'netBuild -t {dname}_Network_{res} -u "Network" -d {dname} $SOURCE $TARGET'.format(dname=dname, res=STNres))

    # export to asciigrid
    env.Command(
            source = os.path.join(output, '{}_Network_{}.gdbn'.format(dname, STNres)),
            target = os.path.join(work, '{}_Network_{}.asc'.format(dname, STNres)),
            action = 'rgis2asciigrid $SOURCE $TARGET')

    # vectorize
    shpfile = os.path.join(output, '{}_{}.shp'.format(dname, STNres)),
    env.Command(
            source = os.path.join(work, '{}_Network_{}.asc'.format(dname, STNres)),
            target = shpfile,
            action = lib.vectorize_joined_basins,
            delta=delta)
    shpfiles.append(shpfile)

# merge individual basin shapefiles
env.Command(
        source=shpfiles,
        target=os.path.join(topoutput, 'full_domain', 'full_domain_{}.shp'.format(STNres)),
        action=lib.merge_shpfiles)

# dissolve and buffer
env.Command(
        source=[os.path.join(topoutput, 'full_domain', 'full_domain_{}.shp'.format(STNres)),
                '/Users/ecr/ztessler/data/Coastline/GSHHG/gshhg-shp-2.3.6/GSHHS_shp/f/GSHHS_f_L1.shp'],
        target=os.path.join(topoutput, 'full_domain', 'full_domain_buff_{}.shp'.format(STNres)),
        action=lib.buffer_shpfile,
        buffer_dist=0.25)
