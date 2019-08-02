-- merge part files which are generated in problem3 into one tab separated file.
rm -f top5_all.tsv 
hdfs dfs -getmerge /user/maria_dev/final/amazon_top5/ top5_all.tsv
-- move this file to hdfs storage
hdfs dfs -rm /user/maria_dev/final/amazon_top5_all/top5_all.tsv
hdfs dfs -copyFromLocal top5_all.tsv /user/maria_dev/final/amazon_top5_all/

