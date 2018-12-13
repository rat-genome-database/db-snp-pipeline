# runs loading for dbSnp for rat, build 138

java -Dspring.config=../properties/default_db.xml -jar DbSnpLoad.jar -data_dir /ref/genomes/human/dbSnp/dbSnp138 -map_key 17 -build dbSnp138 -group_label GRCh37.p10
