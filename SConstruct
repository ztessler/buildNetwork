# vim: fileencoding=UTF-8
# vim: filetype=python

### TO RUN:
# DELTA=[Mekong] STNdomain=[Global] STNres=[06min] scons -i
# (need -i to ignore the core dump in netBuild and keep going. output is fine)

import os
import sys
import lib
import hashlib

SetOption('max_drift', 1)

env = Environment(ENV = {'PATH' : os.environ['PATH'],
                         'GDAL_DATA': os.environ['GDAL_DATA'],
                         },
                  SHELL = '/bin/bash')
env.Decider('MD5-timestamp')

topwork = 'work'
topoutput = 'output'

#deltas = ['Mekong', 'Chao Phraya']
alldeltas = ['Mekong', 'Chao Phraya', 'Irrawaddy', 'Ganges', 'Brahmani', 'Mahanadi', 'Godavari',
             'Krishna', 'SSEA']

STNdomain = os.environ.get('STNdomain', 'Asia')
STNres = os.environ.get('STNres', '06min')
if 'DELTA' in os.environ:
    deltas = [os.environ['DELTA']]
    # use "SSEA" for complete SSEA coastline
else:
    deltas = alldeltas

initial_network = '/asrc/RGISarchive2/{domain}/Network/HydroSTN30/{res}/Static/{domain}_Network_HydroSTN30_{res}_Static.gdbn.gz'.format(domain=STNdomain, res=STNres)


def myCommand(target, source, action, **kwargs):
    '''
    env.Command wrapper that forces env override arguments to be sconsign
    signature database. Wraps all extra kwargs in env.Value nodes and adds
    them to the source list, after the existing sources. Changing the extra
    arguments will cause the target to be rebuilt, as long as the data's string
    representation changes.
    '''
    def hash(v):
        # if this is changed then all targets with env overrides will be rebuilt
        return hashlib.md5(repr(v).encode('utf-8')).hexdigest()
    if not isinstance(source, list):
        source = [source]
    if None in source:
        source.remove(None)
    kwargs['nsources'] = len(source)
    source.extend([env.Value('{}={}'.format(k,hash(v))) for k,v in kwargs.items()])
    return env.Command(target=target, source=source, action=action, **kwargs)

shpfiles = []

for delta in deltas:
    dname = delta.replace(' ','_')
    work = os.path.join(topwork, dname, STNres)
    output = os.path.join(topoutput, dname, STNres)

    inidomain_work = os.path.join(topwork, STNdomain, STNres)

    env.Command(
            source = initial_network,
            target = os.path.join(inidomain_work, '{domain}_{res}.1.gdbc'.format(domain=STNdomain, res=STNres)),
            action='netCells2Grid -f BasinID -t BasinID -u BasinID -d Global $SOURCE $TARGET')
    env.Command(
            source = os.path.join(inidomain_work, '{domain}_{res}.1.gdbc'.format(domain=STNdomain, res=STNres)),
            target = os.path.join(inidomain_work, '{domain}_{res}.2.gdbc'.format(domain=STNdomain, res=STNres)),
            action='grdRenameLayers -r 1 XXXX $SOURCE $TARGET')
    env.Command(
            source = os.path.join(inidomain_work, '{domain}_{res}.2.gdbc'.format(domain=STNdomain, res=STNres)),
            target = os.path.join(inidomain_work, '{domain}_{res}.nc'.format(domain=STNdomain, res=STNres)),
            action='rgis2netcdf $SOURCE $TARGET')
    env.Command(
            source = os.path.join(inidomain_work, '{domain}_{res}.nc'.format(domain=STNdomain, res=STNres)),
            target = os.path.join(inidomain_work, '{domain}_{res}.tif'.format(domain=STNdomain, res=STNres)),
            action = lib.georef_nc_to_tif)

    if delta != 'SSEA':
        # get delta extents
        # group all the little shapes together (geo.group_delta_shps)
        env.Command(
                source = '/Users/ecr/ztessler/data/deltas_LCLUC/maps/global_map_shp/global_map.shp',
                target = os.path.join(work, '{}.json'.format(dname)),
                action = lib.group_delta_shps,
                delta = dname)

        # find all contributing basins (geo.contributing basins)
        # write those to file, or print out
        env.Command(
                source = [os.path.join(work, '{}.json'.format(dname)),
                          os.path.join(inidomain_work, '{}_{}.tif'.format(STNdomain, STNres))],
                target = os.path.join(work, '{}_keep_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
                action = lib.delta_basins,
                delta = dname)
        env.Command(
                source = [os.path.join(work, '{}_keep_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
                          initial_network],
                target = os.path.join(work, '{}_Network_{}_{}.gdbt'.format(dname, STNres, STNdomain)),
                action = 'xargs -a ${SOURCES[0]} -n 1 -I{} echo BasinID != {} "&&" | tr "\\n" " " | xargs -I{} tblDeleteRec -a DBCells -c \"{} BasinID != -8888\" ${SOURCES[1]} $TARGET')
    else:
        # extract basins from network (gives a table)
        # use new tblAddIDXY to add mouthxy coords to basins
        # then use tblJoinTables to get mouthxy coords onto cell table
        # then use simple tblDeleteRec to remove cells that are outside target bounding box
        bbox = (72, 110, 6, 24)
        env.Command(
                source=initial_network,
                target=os.path.join(inidomain_work, 'network_with_mouthXY.gdbn'),
                action='tblAddIdXY -a DBItems -f BasinID -x MouthXCoord -y MouthYCoord $SOURCE $TARGET')
        env.Command(
                source=os.path.join(inidomain_work, 'network_with_mouthXY.gdbn'),
                target=os.path.join(inidomain_work, 'network_with_mouthXY_on_dbcells.gdbn'),
                action='tblJoinTables -e DBCells -o DBItems -r BasinID -j BasinID $SOURCE $TARGET')
        env.Command(
                source=os.path.join(inidomain_work, 'network_with_mouthXY_on_dbcells.gdbn'),
                target=os.path.join(work, '{}_Network_{}_{}.gdbt'.format(dname, STNres, STNdomain)),
                action='tblDeleteRec -a DBCells -c "MouthXCoord < {0} || MouthXCoord > {1} || MouthYCoord < {2} || MouthYCoord > {3}" $SOURCE $TARGET'.format(*bbox))

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
            target = os.path.join(output, '{}_Network_{}.0.gdbn'.format(dname, STNres)),
            action = 'netBuild -t {dname}_Network_{res} -u "Network" -d {dname} $SOURCE $TARGET'.format(dname=dname, res=STNres))

    # add XY to cells, needed later in osm_rivers, doesnt hurt to have
    env.Command(
            source= os.path.join(output, '{}_Network_{}.0.gdbn'.format(dname, STNres)),
            target= os.path.join(output, '{}_Network_{}.gdbn'.format(dname, STNres)),
            action = 'tblAddIdXY -a DBCells $SOURCE $TARGET')

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
            delta = dname)
    shpfiles.append(shpfile)

if deltas == alldeltas:
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
