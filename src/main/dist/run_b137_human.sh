# runs loading for dbSnp for rat, build 137 on dev database

java -Dspring.config=../properties/default_db.xml -jar DbSnpLoad.jar -data_dir /ref/genomes/human/dbSnp/dbSnp137 -map_key 17 -build dbSnp137 -group_label GRCh37.p5
