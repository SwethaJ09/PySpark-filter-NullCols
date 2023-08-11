#method-1
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#Create a pyspark session
spark: SparkSession = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
#initialise a dataset
data = [
    ("Swetha",None,"F",19),
    ("Agalya","IND","F",20),
    ("Annie",None,"F",None)
]

columns = ["NAME","COUNTRY","GENDER","AGE"]

#create a dataframe using spark

df =spark.createDataFrame(data,columns)

#display the dataframe
df.printSchema()
df.show()


df.filter("COUNTRY is NULL").show()

df.filter("NOT COUNTRY is NULL").show()

#method-2

#import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark: SparkSession = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
#initialise dataframe
data = [
    ("Swetha",None,"F",19),
    ("Agalya","IND","F",20),
    ("Annie",None,"F",None)
]

columns = ["NAME","COUNTRY","GENDER","AGE"]

#create a dataframe
df =spark.createDataFrame(data,columns)


#display the dataframe
df.printSchema()
df.show()

#filter the values
df.filter(df.COUNTRY.isNotNull()).show()

df.filter(df.COUNTRY.isNull()).show()
