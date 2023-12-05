
from pyspark.sql import SparkSession



def startSparkSeesion():
    spark = SparkSession.builder.appName("harriTask").getOrCreate()
    print(spark.version)
    return spark



def readDataSetBySpark(spark):
    df_spark_car = spark.read.option('header', 'true').csv('taskfiles/cars.csv')
    df_spark_sheet = spark.read.option('header', 'true').csv('taskfiles/Sheet1.csv')
    df_sparkTotalThefts = spark.read.option('header', 'true').csv('taskfiles/2015_State_Top10Report_wTotalThefts.csv')
    print(type(df_spark_car))
    print(df_spark_car.printSchema())
    return df_spark_car , df_spark_sheet, df_sparkTotalThefts

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = startSparkSeesion()
    df_spark_car, df_spark_sheet,df_sparkTotalThefts = readDataSetBySpark(spark)
    print(df_spark_car.show())
    print(df_spark_sheet.show())
    print(df_sparkTotalThefts.show())

