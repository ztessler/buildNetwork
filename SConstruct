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
alldeltas = ['Mekong', 'Chao Phraya', 'Irrawaddy', 'Ganges', 'Brahmani', 'Mahanadi', 'Godavari', 'Krishna']

STNdomain = os.environ.get('STNdomain', 'Asia')
STNres = os.environ.get('STNres', '06min')
if 'DELTA' in os.environ:
    deltas = [os.environ['DELTA']]
    # use "SSEA" for complete SSEA coastline
else:
    deltas = alldeltas

### Get initial network by opening this file (global version):
# '/asrc/ecr/balazs/Projects/2018/2018-10_HydroSTN30v100/RGISlocal/{domain}/Network/HydroSTN30ext/{res}/Static/{domain}_Network_HydroSTN30ext_{res}_Static.gdbn.gz'.format(domain=STNdomain, res=STNres)
# and adding Mouth coords, saving to similar path in RGISlocal
initial_network = os.path.join('../../RGISlocal/Global/Network/HydroSTN30ext/{res}/Static/Global_Network_HydroSTN30ext_{res}_Static.gdbn'.format(res=STNres))


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

    # get delta extents
    # group all the little shapes together (geo.group_delta_shps)
    if delta != 'SSEA':
        env.Command(
                source = '/Users/ecr/ztessler/data/deltas_LCLUC/maps/global_map_shp/global_map.shp',
                target = os.path.join(work, '{}.json'.format(dname)),
                action = lib.group_delta_shps,
                delta = delta)

        # find all contributing basins (geo.contributing basins)
        # write those to file, or print out
        env.Command(
                source = [os.path.join(work, '{}.json'.format(dname)),
                          os.path.join(inidomain_work, '{}_{}.tif'.format(STNdomain, STNres))],
                target = os.path.join(work, '{}_keep_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
                action = lib.delta_basins,
                delta = delta)
        env.Command(
                source = [os.path.join(work, '{}_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
                          initial_network],
                target = os.path.join(work, '{}_Network_{}_{}.gdbt'.format(dname, STNres, STNdomain)),
                action = 'xargs -a ${SOURCES[0]} -n 1 -I{} echo BasinID != {} "&&" | tr "\\n" " " | xargs -I{} tblDeleteRec -a DBCells -c \"{} BasinID != -8888\" ${SOURCES[1]} $TARGET')
    else:
        # walk along SSEA coastline finding all basins that dischage to coast
        mouths = os.path.join(inidomain_work, 'mouth_cells_global_{}.csv'.format(STNres))
        env.Command(
                source=initial_network,
                target=mouths,
                action='rgis2table $SOURCE | sed \'s/\\t/,/g\' > $TARGET')
        bbox = (72, 110, 6, 24)
        myCommand(
                source=mouths,
                target=os.path.join(work, '{}_keep_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
                action=lib.mouths_in_ssea,
                bbox=bbox) # (lonmin, lonmax, latmin, latmax)

        # extract basins from network (gives a table)
        # NOTE this command deletes all not-equal-to-ones-to-keep. ok if few basins to keep. too many overflows xargs
        #env.Command(
                #source = [os.path.join(work, '{}_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
                          #initial_network],
                #target = os.path.join(work, '{}_Network_{}_{}.gdbt'.format(dname, STNres, STNdomain)),
                #action = 'xargs -a ${SOURCES[0]} -n 1 -I{} echo BasinID != {} "&&" | tr "\\n" " " | xargs -I{} tblDeleteRec -a DBCells -c \"{} BasinID != -8888\" ${SOURCES[1]} $TARGET')
        # NOTE use these for lots of basins to keep
        #env.Command(
                #source=initial_network,
                #target=os.path.join(work, '{}_all_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
                #action='rgis2table $SOURCE | FCut -f ID | tail -n +2 > $TARGET')
        #env.Command(
                #source=[os.path.join(work, '{}_all_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
                        #os.path.join(work, '{}_keep_basins_{}_{}.txt'.format(dname, STNres, STNdomain))],
                #target=os.path.join(work, '{}_delete_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
                #action='comm -23 <(sort ${SOURCES[0]}) <(sort ${SOURCES[1]}) | sort -n > $TARGET')
        # this command splits the long list of basins to delete up into chunks and runs tblDeleteRec several times
        #env.Command(
                #source = [os.path.join(work, '{}_delete_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
                          #initial_network],
                #target = os.path.join(work, '{}_Network_{}_{}.gdbt'.format(dname, STNres, STNdomain)),
                #action=['cp ${SOURCES[1]} $TARGET',
                        #'awk \'{printf "BasinID == %s || \\n", $$1}\' ${SOURCES[0]} | xargs -L1000 -d "\\n" | xargs -I{} tblDeleteRec -a DBCells -c \"{} BasinID == -8888\" $TARGET $TARGET'])
        # NOTE but that's too slow so instead first select in chunks, then delete selection
        #env.Command(
                #source = [os.path.join(work, '{}_keep_basins_{}_{}.txt'.format(dname, STNres, STNdomain)),
                          #initial_network],
                #target = os.path.join(work, '{}_Network_{}_{}.gdbt'.format(dname, STNres, STNdomain)),
                #action=[# copy to ramdisk
                        #'cp ${SOURCES[1]} /dev/shm/network',
                        ## select all rows
                        #'tblSelectRec -a DBCells -m select /dev/shm/network /dev/shm/network',
                        ## unselect rows to keep, with a counter and progress estimates
                        #'echo $$(wc -l ${SOURCES[0]}) | cut -d" " -f 1 | tee /tmp/nlines',
                        #'echo 0 > /tmp/count',
                        #'awk \'{printf "BasinID == %s || \\n", $$1}\' ${SOURCES[0]} | xargs -L50 -d "\\n" | xargs -I{} bash -c \'i=$$(cat /tmp/count); n=$$(cat /tmp/nlines); tblSelectRec -a DBCells -m unselect -f selection -c \"{} BasinID == -8888\" /dev/shm/network /dev/shm/network; i=$$((i+1)); echo "$$(date +%H:%M:%S), Iteration: $${i}, Selected $$((50*i)) basins, $$(((50*i*100)/n))% done."; echo $$i>/tmp/count\'',
                        ## delete remaining selected rows
                        #'tblDeleteRec -a DBCells -s /dev/shm/network /dev/shm/network'
                        ## copy back to TARGET
                        #'mv /dev/shm/network $TARGET'])
        # NOTE thats also crazy slow
        # another way: use new tblAddIDXY to add mouthxy coords to basins
        # then use tblJoinTables to get mouthxy coords onto cell table
        # then use simple tblDeleteRec to remove cells that are outside original bounding box
        env.Command(
                source=initial_network,
                target=os.path.join(inidomain_work, 'network_with_mouthXY.gdbn'),
                action='/Users/ecr/ztessler/opt/ghaas_anthro/ghaas/bin/tblAddIdXY -a DBItems -f BasinID -x MouthXCoord -y MouthYCoord $SOURCE $TARGET')
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
