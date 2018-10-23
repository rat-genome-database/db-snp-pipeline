# runs loading for dbSnp for human, build 147 for GRCh38.p2 on dev database
#
# Note: you need to define system env variable jdk.xml.totalEntitySizeLimit=0
#   the default is 50,000,000, and once XML parser will read 50M entities it will throw an exception
#   and processing will break; setting this variable to 0 means 'no limit'
#   so XML files for biggest chromosomes (chr1 and chr2) could be processed entirely
#
APPDIR=/home/rgddata/pipelines/DbSnpLoad
cd $APPDIR

java -Dspring.config=../properties/default_db.xml \
   -Dlog4j.configuration=file://$APPDIR/log4j.properties \
   -Djdk.xml.totalEntitySizeLimit=0 \
   -jar DbSnpLoad.jar \
   -data_dir /data/dbSnp/dbSnp147_human \
   -map_key 38 \
   -build dbSnp147 \
   -group_label GRCh38.p2 2>&1 | tee b147_human38.log