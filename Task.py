import os
import shutil
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, split, concat_ws,coalesce,desc
from pyspark.sql.types import StringType


# Start Spark Session
def start_spark_session():
    spark = SparkSession.builder.appName("CarOriginExtraction").getOrCreate()
    return spark


# Read Data Set by Spark
def read_data_set_by_spark(spark, file_paths):
    dfs = [spark.read.option('header', True).csv(file_path) for file_path in file_paths]
    return tuple(dfs)


# Call API for Country
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


# Convert the function to a UDF
call_api_for_country_udf = udf(call_api_for_country, StringType())


# Extract Car Model and Origin
def extract_car_model_and_origin(api_url, df_sheet, output_base_path):

    car_models = df_sheet.select(col('Make_Model')).distinct()
    car_models_with_origin = car_models.withColumn(
        'Country_of_Origin',
        call_api_for_country_udf(split(col('Make_Model'), ' ').getItem(0), lit(api_url))
    )
    car_models_with_origin.cache()
    newDataSet = car_models_with_origin

    car_models_with_origin = car_models_with_origin.repartition('Country_of_Origin')
    countries = [row.Country_of_Origin for row in car_models_with_origin.select('Country_of_Origin').distinct().collect()]

    def write_partition_to_file(country):
        country_df = car_models_with_origin.filter(col('Country_of_Origin') == country)
        file_path = f'{output_base_path}/country_{country}.csv'
        country_df.drop('Country_of_Origin').write.mode('overwrite').csv(file_path)

    for country in countries:
        write_partition_to_file(country)

    return newDataSet


def update_dataset(original_df, updated_df):
    # Define the key columns (all columns except 'Rank')
    key_cols = [c for c in original_df.columns if c != 'Rank' and c !='Thefts']

    # Create a composite key for both DataFrames
    generate_key = concat_ws('_', *[col(c) for c in key_cols])
    original_df = original_df.withColumn('composite_key', generate_key)
    updated_df = updated_df.withColumn('composite_key', generate_key)

    # Rename all columns in the updated DataFrame except for the composite key
    for col_name in updated_df.columns:
        if col_name != 'composite_key':
            updated_df = updated_df.withColumnRenamed(col_name, col_name + "_updated")

    # Perform a full outer join on the composite key
    full_joined_df = original_df.join(updated_df, 'composite_key', 'full_outer')

    # Coalesce each column: prefer updated data if available, else use original
    for col_name in original_df.columns:
        if col_name != 'composite_key':
            full_joined_df = full_joined_df.withColumn(
                col_name,
                coalesce(col(col_name + "_updated"), col(col_name))
            )

    # Drop the composite key and updated columns
    for col_name in updated_df.columns:
        if col_name != 'composite_key':
            full_joined_df = full_joined_df.drop(col_name)

    print("Update Complete")
    return full_joined_df.drop('composite_key')


def spark_sql_query(df_report, df_carModel_Country):
    # Top 5 stolen car models
    top_stolen_models = (
        df_report
        .groupBy("Make_Model")
        .sum("Thefts")
        .withColumnRenamed("sum(Thefts)", "TotalThefts")
        .orderBy(desc("TotalThefts"))
        .limit(5)
    )

    # Top 5 states with the most stolen cars
    top_stolen_states = (
        df_report
        .groupBy("State")
        .sum("Thefts")
        .withColumnRenamed("sum(Thefts)", "TotalThefts")
        .orderBy(desc("TotalThefts"))
        .limit(5)
    )

    # join the DataFrames on 'Make_Model'
    joined_df = df_report.join(df_carModel_Country, 'Make_Model')
    # group by 'Country_of_Origin' and count occurrences
    country_counts = joined_df.groupBy('Country_of_Origin').count()
    # sort the results in descending order of count
    sorted_country_counts = country_counts.orderBy(desc('count'))

    # Show the results
    print("Top 5 stolen car models:")
    top_stolen_models.show()

    print("\nTop 5 states with the most stolen cars:")
    top_stolen_states.show()

    print("\nMost common country of origin for car models purchased by Americans:")
    sorted_country_counts.show()


# Main Execution Block
if __name__ == '__main__':

    file_paths = ['taskfiles/cars.csv', 'taskfiles/Sheet1.csv', 'taskfiles/2015_State_Top10Report_wTotalThefts.csv']
    updated_file_path = 'taskfiles/Sheet1.csv'
    api_url = 'http://127.0.0.1:8080/cars/getbyBrand'
    output_base_path1 = 'taskfiles/output_API'  # where each car model with its country
    output_base_path2 = 'taskfiles/output_updated'  # the result of updated data from sheet1

    spark_session = start_spark_session()
    dfs = read_data_set_by_spark(spark_session, file_paths)
    df_spark_car, df_spark_sheet, df_sparkTotalThefts = dfs

    if not os.path.exists(output_base_path1):
        os.makedirs(output_base_path1)
    else:
        # If the directory already exists, delete it and recreate
        shutil.rmtree(output_base_path1)
        os.makedirs(output_base_path1)

    if not os.path.exists(output_base_path2):
        os.makedirs(output_base_path2)
    else:
        # If the directory already exists, delete it and recreate
        shutil.rmtree(output_base_path2)
        os.makedirs(output_base_path2)

    updated_dataset = update_dataset(df_sparkTotalThefts, df_spark_sheet)
    #for count query
    updated_dataset = updated_dataset.withColumn("Thefts", col("Thefts").cast("int"))

    updated_dataset.write.option("header", "true").mode("overwrite").csv(output_base_path2)

    df_carModel_Country = extract_car_model_and_origin(api_url, updated_dataset, output_base_path1)

    spark_sql_query(updated_dataset, df_carModel_Country)

