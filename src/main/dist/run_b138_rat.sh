# runs loading for dbSnp for rat, build 138

java -Dspring.config=../properties/default_db.xml -jar DbSnpLoad.jar -data_dir /data/rat/dbSnp/dbSnp138 -map_key 70 -build dbSnp138 -group_label Rnor_5.0
