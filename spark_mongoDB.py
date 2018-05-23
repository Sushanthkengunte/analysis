from pyspark.sql import SparkSession, Row , functions

def parseLine(line):
    fields = line.split(",")
    return Row(user_id = int(fields[0]),age = int(fields[1]),gender = fields[2],occupation = fields[3],zip = fields[4])
if __name__ == '__main__':
    spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()

    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.user")

    users = lines.map(parseLine)

    # grouped = weapons.reduceByKey(lambda x,y: x+y)
    #
    # print(grouped.take(10))


    userDataset = spark.createDataFrame(users)

    userDataset.write.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/movielens.users").mode("append").save()
    readUsers = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/movielens.users").load()
    readUsers.createOrReplaceTempView("users")
    sqlDF = spark.sql("SELECT * FROM users WHERE age < 20")
    sqlDF.show()
    spark.stop()