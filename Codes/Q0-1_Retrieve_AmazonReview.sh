aws s3 cp s3://amazon-reviews-pds/tsv/amazon_reviews_us_Grocery_v1_00.tsv.gz .                               
aws s3 cp s3://amazon-reviews-pds/tsv/amazon_reviews_us_Baby_v1_00.tsv.gz .   

gunzip amazon_reviews_us_Grocery_v1_00.tsv.gz . 
gunzip amazon_reviews_us_Baby_v1_00.tsv.gz . 

cat amazon_reviews_us_Grocery_v1_00.tsv amazon_reviews_us_Baby_v1_00.tsv > amazon_reviews_all.tsv

hdfs dfs -rm /user/maria_dev/final/amazon/amazon_reviews_all.tsv
hdfs dfs -copyFromLocal amazon_reviews_all.tsv /user/maria_dev/final/amazon/
