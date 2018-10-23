# runs loading for dbSnp for rat, build 136 on dev database
# tmp dir
#java -Dspring.config=../properties/default_db.xml -jar DbSnpLoad.jar -data_dir /data/rat/dbSnp/dbSnp136 -map_key 60 -build dbSnp136 -group_label RGSC_v3.4
# correct src dir
java -Dspring.config=../properties/default_db.xml -jar DbSnpLoad.jar -data_dir /ref/genomes/rat/dbSnp/dbSnp136 -map_key 60 -build dbSnp136 -group_label RGSC_v3.4
