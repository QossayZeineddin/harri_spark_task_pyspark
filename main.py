# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from pyspark.sql import SparkSession

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

def startSparkSeesion():
    spark = SparkSession.builder.appName("harriTask").getOrCreate()
    print(spark.version)
    df_spark = spark.read.csv('taskfiles/cars.csv')
    print(df_spark.show())

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    startSparkSeesion()


