# runs loading for dbSnp for mouse, build 131 on dev database
# tmp dir
java -Dspring.config=../properties/default_db.xml -jar DbSnpLoad.jar -data_dir /data/mouse/dbSnp/dbSnp137 -map_key 35 -build dbSnp137 -group_label GRCm38
# correct src dir
#java -Dspring.config=../properties/default_db.xml -jar DbSnpLoad.jar -data_dir /ref/genomes/mouse/dbSnp/dbSnp137 -map_key 35 -build dbSnp137 -group_label GRCm38
