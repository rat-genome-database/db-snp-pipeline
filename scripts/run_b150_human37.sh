# runs loading for dbSnp for human, build 150 for GRCh37.p13 on dev database
#   but only for variants with clinical significance information present
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
   -data_dir /data/dbSnp/dbSnp150_human37 \
   -map_key 17 \
   -build dbSnp150 \
   -with_clinical_significance \
   -group_label GRCh37.p13 2>&1 | tee b150_human37.log