from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StringType
import requests

def start_spark_session():
    spark = SparkSession.builder.appName("CarOriginExtraction").getOrCreate()
    return spark

def read_data_set_by_spark(spark, file_paths):
    dfs = [spark.read.option('header', 'true').csv(file_path) for file_path in file_paths]
    return tuple(dfs)


def call_api_for_country(car_brand, api_url):
    try:
        # Make a simple GET request to the API
        response = requests.get(api_url + f'/{car_brand}')

        # Assuming the API returns a list of dictionaries
        json_response_list = response.json()

        print(f'Response for {car_brand}: {json_response_list}')  # Add this line

        # Check if the response is a non-empty list
        if json_response_list:
            # Extract the first item from the list
            json_response = json_response_list[0]

            # Extract the country of origin from the response
            country_of_origin = json_response.get('countryOfOrigin', 'Unknown')
        else:
            # If the list is empty, set country_of_origin to 'Unknown'
            country_of_origin = 'Unknown'

        return country_of_origin
    except Exception as e:
        # Handle exceptions gracefully
        return f'Error: {str(e)}'


def extract_car_model_and_origin(spark, api_url, df_car, output_csv_path):
    # Select only the relevant columns
    car_models = df_car.select(col('Car_Brand')).distinct()

    # Add a new column 'Country_of_Origin' using the API
    car_models_with_origin = car_models.withColumn(
        'Country_of_Origin',
        call_api_for_country_udf(col('Car_Brand'), lit(api_url))
    )

    # Save the results to a new CSV file
    car_models_with_origin.write.mode('overwrite').csv(output_csv_path)




# Convert the function to a UDF
call_api_for_country_udf = udf(call_api_for_country, StringType())

if __name__ == '__main__':
    # Specify the file paths
    file_paths = ['taskfiles/cars.csv', 'taskfiles/Sheet1.csv', 'taskfiles/2015_State_Top10Report_wTotalThefts.csv']

    spark_session = start_spark_session()
    dfs = read_data_set_by_spark(spark_session, file_paths)
    df_spark_car, df_spark_sheet, df_sparkTotalThefts = dfs
    print(df_spark_car.printSchema())

    # Replace this path with the actual path to your desired output location
    output_csv_path = 'taskfiles/test'

    # Replace 'http://localhost:8080/api/cars/search' with the actual URL of your API
    api_url = 'http://127.0.0.1:8080/cars/getbyBrand'

    extract_car_model_and_origin(spark_session, api_url, df_spark_car, output_csv_path)
    print("Doneeee")
