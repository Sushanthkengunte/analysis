from pyspark.sql import SparkSession, Row , functions

def parseLine(line):
    fields = line.split(",")
    return Row(weaponName = fields[0],count = 1)

if __name__ == '__main__':
    spark = SparkSession.builder.appName("weaponDistribution").getOrCreate()

    lines = spark.sparkContext.textFile("file:///home/maria_dev/deaths.csv")

    weapons = lines.map(parseLine)

    # grouped = weapons.reduceByKey(lambda x,y: x+y)
    #
    # print(grouped.take(10))

    weaponDistribution = spark.createDataFrame(weapons)


    distribution = weaponDistribution.groupBy("weaponName").count()
    topten = distribution.take(10)
    # distribution.toPandas().to_csv('example')

    print(topten)

