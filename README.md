# Extract river network for specific delta domain

Some hardcoded paths in SConstruct will need editing. Needs the GHAAS and RGIS tools installed.

Building of output RGIS river network files is driven by SCons, usually used for software
compiliation but works very well for data processing pipelines as well.

Run a series of deltas and resolutions by editing and running runall.sh

Or process specific deltas and resolutions as:

`STNdomain=Mekong STNres=06min scons -i`

Note, need to run scons with the -i flag (continue running after errors) due to bug in netBuild,
which segfaults, but only after the needed files are writen to disk, so its OK to just continue the
data pipeline.
