# call this cript by using spark-submit command

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder.appName("Spark").getOrCreate()

#Define schema for 'top5_all.tsv' which was generated in question3.
schema = StructType([ StructField('product_category', StringType(), False), StructField('product_id',  StringType(), False), StructField('product_title', StringType(), False),  StructField('character', StringType(), True), StructField('rank', IntegerType(), True), StructField('rating', IntegerType(), True), StructField('review_headline', StringType(), True),StructField('review_body', StringType(), True)])
top5 = spark.read.format("csv").option("header", "false").option("delimiter", "\t").schema(schema).load("/user/maria_dev/final/amazon_top5_all/top5_all.tsv")

#Create RDD for review comments for text processing
review = top5.select("review_body").rdd

# Need to take line[0] because RDD is generated from SparkSQl dataframe.
mapWords = review.flatMap(lambda line: line[0].split())
mapFirst = mapWords.map(lambda word: (word.lower(),1))
reduceSecond = mapFirst.reduceByKey(lambda x,y: x+y)


# Create dataframe
schema_word_cnt = StructType([ StructField('word', StringType(), False), StructField('cnt', IntegerType(), False)])

sqlContext = SQLContext(spark)
word_cnt = sqlContext.createDataFrame(reduceSecond, schema_word_cnt)

# Load adjective list
adj = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/maria_dev/final/dict/Adjectives.csv")

# Create temporary view
word_cnt.createOrReplaceTempView("word_cnt")
adj.createOrReplaceTempView("adj")

# Query all adjectives used in review comments
spark.sql("select a.word, a.cnt, b.Category from word_cnt a inner join adj b on lcase(a.word) = lcase(b.Adjective) order by a.cnt desc").show()