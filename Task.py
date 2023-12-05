import os
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, split
from pyspark.sql.types import StringType

def start_spark_session():
    spark = SparkSession.builder.appName("CarOriginExtraction").getOrCreate()
    return spark

def read_data_set_by_spark(spark, file_paths):
    dfs = [spark.read.option('header', 'true').csv(file_path) for file_path in file_paths]
    return tuple(dfs)

def call_api_for_country(car_brand, api_url):
    try:
        response = requests.get(api_url + f'/{car_brand}')
        json_response_list = response.json()

        if json_response_list:
            json_response = json_response_list[0]
            country_of_origin = json_response.get('countryOfOrigin', 'Unknown')
        else:
            country_of_origin = 'Unknown'

        return country_of_origin
    except Exception as e:
        return f'Error: {str(e)}'

def extract_car_model_and_origin(spark, api_url, df_sheet, output_base_path):
    car_models = df_sheet.select(col('Make_Model')).distinct()

    # Use withColumn and a lambda function to apply the UDF to each row
    car_models_with_origin = car_models.withColumn(
        'Country_of_Origin',
        call_api_for_country_udf(split(col('Make_Model'), ' ').getItem(0), lit(api_url))
    )
    print(car_models_with_origin.show())
    # Repartition by 'Country_of_Origin' for better parallelism
    car_models_with_origin = car_models_with_origin.repartition('Country_of_Origin')

    # Collect distinct countries to process each partition separately
    countries = [row.Country_of_Origin for row in car_models_with_origin.select('Country_of_Origin').distinct().collect()]

    def write_partition_to_file(country):
        country_df = car_models_with_origin.filter(col('Country_of_Origin') == country)
        file_path = f'{output_base_path}/country_{country}.csv'
        country_df.drop('Country_of_Origin').write.mode('overwrite').csv(file_path)

    # Apply the function to each distinct country
    for country in countries:
        write_partition_to_file(country)

# Convert the function to a UDF
call_api_for_country_udf = udf(call_api_for_country, StringType())

if __name__ == '__main__':
    file_paths = ['taskfiles/cars.csv', 'taskfiles/Sheet1.csv', 'taskfiles/2015_State_Top10Report_wTotalThefts.csv']
    api_url = 'http://127.0.0.1:8080/cars/getbyBrand'
    output_base_path = 'taskfiles/output'

    spark_session = start_spark_session()
    dfs = read_data_set_by_spark(spark_session, file_paths)
    df_spark_car, df_spark_sheet, df_sparkTotalThefts = dfs
    print(df_sparkTotalThefts.printSchema())

    if not os.path.exists(output_base_path):
        os.makedirs(output_base_path)


    extract_car_model_and_origin(spark_session, api_url, df_sparkTotalThefts, output_base_path)
    print("Doneeee")
