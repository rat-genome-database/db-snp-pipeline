# runs loading for dbSnp for rat, build 146 on dev database
#
# note: downloaded files should be put to readonly storage to:
#   /ref/genomes/rat/dbSnp/dbSnp146
#
APPDIR=/home/rgddata/pipelines/DbSnpLoad
cd $APPDIR

java -Dspring.config=../properties/default_db.xml \
   -Dlog4j.configuration=file://$APPDIR/log4j.properties \
   -jar DbSnpLoad.jar \
   -data_dir /data/dbSnp/dbSnp146_rat \
   -map_key 360 \
   -build dbSnp146 \
   -group_label Rnor_6.0
