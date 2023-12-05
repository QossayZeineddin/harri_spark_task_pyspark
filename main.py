import os
import shutil

import requests
from pyspark import SparkContext

def start_spark_context():
    sc = SparkContext(appName="CarOriginExtraction")
    return sc

def read_data_set_by_spark(sc, file_paths):
    rdds = [sc.textFile(file_path) for file_path in file_paths]
    return tuple(rdds)

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

def extract_car_model_and_origin(sc, api_url, rdd_sheet, output_base_path):
    car_models = rdd_sheet.map(lambda row: row.split(',')[2]).distinct()

    def call_api_for_country_rdd(car_model):
        return call_api_for_country(car_model.split(' ')[0], api_url)

    car_models_with_origin = car_models.map(lambda car_model: (car_model, call_api_for_country_rdd(car_model)))
    # Cache the RDD for later reuse
    car_models_with_origin.cache()

    print(car_models_with_origin.collect())

    # Repartition by 'Country_of_Origin' for better parallelism using custom partitioner
    custom_partitioner = CustomHashPartitioner(2)
    car_models_with_origin = car_models_with_origin.partitionBy(2, custom_partitioner)

    # Collect distinct countries to process each partition separately
    countries = car_models_with_origin.map(lambda x: x[1]).distinct().collect()

    def write_partition_to_file(output_base_path):
        def _write_partition(country):
            country_rdd = car_models_with_origin.filter(lambda x: x[1] == country)
            file_path = f'{output_base_path}/country_{country}.csv'

            country_rdd.map(lambda x: x[0]).saveAsTextFile(file_path)

        return _write_partition

    # Apply the function to each distinct country
    for country in countries:
        write_partition_to_file(output_base_path)(country)

class CustomHashPartitioner:
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions

    def __call__(self, key):
        return hash(key) % self.num_partitions

if __name__ == '__main__':
    file_paths = ['taskfiles/cars.csv', 'taskfiles/Sheet1.csv', 'taskfiles/2015_State_Top10Report_wTotalThefts.csv']
    api_url = 'http://127.0.0.1:8080/cars/getbyBrand'
    output_base_path = 'taskfiles/output2'

    sc = start_spark_context()
    rdds = read_data_set_by_spark(sc, file_paths)
    rdd_spark_car, rdd_spark_sheet, rdd_sparkTotalThefts = rdds

    if not os.path.exists(output_base_path):
        os.makedirs(output_base_path)
    else:
        # If the directory already exists, delete it and recreate
        shutil.rmtree(output_base_path)
        os.makedirs(output_base_path)

    extract_car_model_and_origin(sc, api_url, rdd_sparkTotalThefts, output_base_path)
    print("Doneeee")




    #However, keep in mind that using parallelize is different from reading data from external sources like files.
    # In files case, it seems we want to parallelize the processing of data within each partition.
    # The parallelize method is typically used when you have a collection of data in memory that you want
    # to distribute across the Spark cluster.Since you are reading data from external sources using textFile,
    # the data is already distributed across partitions based on the underlying Hadoop InputFormat.
    # In this case, it might be more effective to focus on optimizing the processing within each partition
    # rather than explicitly parallelizing the data.
