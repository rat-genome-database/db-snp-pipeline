# runs loading for dbSnp for rat, build 138 on dev database
# tmp dir
#java -Dspring.config=../properties/default_db.xml -jar DbSnpLoad.jar -data_dir /ref/genomes/rat/dbSnp/dbSnp136 -map_key 60 -build dbSnp136 -group_label RGSC_v3.4
# correct src dir
java -Dspring.config=../properties/default_db.xml -jar DbSnpLoad.jar -data_dir /data/rat/dbSnp/dbSnp138 -map_key 70 -build dbSnp138 -group_label Rnor_5.0
