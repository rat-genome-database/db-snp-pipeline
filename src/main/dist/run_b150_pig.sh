# runs loading for dbSnp for pig, build 150 on dev database

java -Dspring.config=../properties/default_db.xml -jar DbSnpLoad.jar -data_dir /ref/genomes/pig/dbSnp/dbSnp150 -map_key 911 -build dbSnp150 -group_label sscrofa_111