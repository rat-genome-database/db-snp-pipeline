# runs loading for dbSnp for mouse, build 131 on dev database

java -Dspring.config=../properties/default_db.xml -jar DbSnpLoad.jar -data_dir /ref/genomes/mouse/dbSnp/dbSnp131 -map_key 18 -build dbSnp131 -group_label MGSCv37
