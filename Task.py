import os
import shutil
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, split, concat_ws, coalesce, desc, count, sum, countDistinct
from pyspark.sql.types import StringType
import logging


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
        response = requests.get(f'{api_url}{car_brand}')
        json_response_list = response.json()
        if json_response_list:
            return json_response_list[0].get('countryOfOrigin', 'Unknown')
        else:
            return 'Unknown'
    except Exception as e:
        return f'Error: {str(e)}'


# Convert the function to a UDF
call_api_for_country_udf = udf(call_api_for_country, StringType())


# Extract Car Model and Origin
# Enhanced extract_car_model_and_origin function
def extract_car_model_and_origin(api_url, df_sheet, output_base_path):
    car_models = df_sheet.select(col('Make_Model')).distinct()
    car_models_with_origin = car_models.withColumn(
        'Country_of_Origin',
        call_api_for_country_udf(split(col('Make_Model'), ' ').getItem(0), lit(api_url))
    ).cache()
    car_models_with_origin.repartition('Country_of_Origin').write.option("header", "true").csv(output_base_path + "/result")

    countries = [row.Country_of_Origin for row in car_models_with_origin.select('Country_of_Origin').distinct().collect()]

    for country in countries:
        country_df = car_models_with_origin.filter(col('Country_of_Origin') == country)
        file_path = f'{output_base_path}/country_{country}.csv'
        country_df.drop('Country_of_Origin').write.mode('overwrite').csv(file_path)

    return car_models_with_origin


def update_dataset(original_df, updated_df):
    # Define the key columns (all columns except 'Rank')
    key_cols = [c for c in original_df.columns if c != 'Rank' and c != 'Thefts']

    # Create a composite key for both DataFrames
    generate_key = concat_ws('_', *[col(c) for c in key_cols])
    original_df = original_df.withColumn('composite_key', generate_key)
    updated_df = updated_df.withColumn('composite_key', generate_key)

    # Perform a left join on the composite key
    full_joined_df = original_df.join(updated_df.select('composite_key', *[col(c).alias(f"{c}_updated") for c in updated_df.columns if c != 'composite_key']), 'composite_key', 'left')

    # Coalesce each column: prefer updated data if available, else use original
    for col_name in original_df.columns:
        if col_name != 'composite_key':
            full_joined_df = full_joined_df.withColumn(
                col_name,
                coalesce(col(col_name + "_updated"), col(col_name))
            )

    # Drop the composite key and updated columns
    full_joined_df = full_joined_df.drop('composite_key', *["{}_updated".format(c) for c in updated_df.columns if c != 'composite_key'])

    print("Update Complete")
    return full_joined_df

#

def create_or_clear_directory(directory_path):
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
    os.makedirs(directory_path)

def spark_sql_query(df_report, df_carModel_Country, output_path):
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

    # Based on the models, what is the most country from where Americans buy their cars?
    # based on data that we have there are two Solution first take the total theft of each
    # country cars and the second just count car models and its country that the american have

    ## first one / the most correct solution
    # join the DataFrames on 'Make_Model'
    joined_df = (
        df_report
        .join(df_carModel_Country, 'Make_Model')
    )
    # Cast 'Thefts' column to integer
    joined_df = joined_df.withColumn("Thefts", joined_df["Thefts"].cast("int"))

    # group by 'Country_of_Origin' and sum of thefts
    country_counts = (
        joined_df
        .groupBy('Country_of_Origin')
        .agg(sum('Thefts').alias('TotalThefts'), count('*').alias('car_models_count_with_out_distinct'))
    )

    # sort the results in descending order of TotalThefts and limit to 1
    sorted_country_counts1 = (
        country_counts
        .orderBy(desc('TotalThefts'))

    )

    ## secand
    # join the DataFrames on 'Make_Model'
    joined_df = df_report.join(df_carModel_Country, 'Make_Model')

    # group by 'Country_of_Origin' and count distinct occurrences of 'Make_Model'
    country_counts = joined_df.groupBy('Country_of_Origin').agg(
        countDistinct('Make_Model').alias('Distinct_Make_Models'))

    # sort the results in descending order of distinct count and limit to 1
    sorted_country_counts2 = country_counts.orderBy(desc('Distinct_Make_Models'))

    #country_counts = joined_df.groupBy('Country_of_Origin').count()
    #sorted_country_counts = country_counts.orderBy(desc('count')).limit(1)

    # Show the results
    print("Top 5 stolen car models:")
    top_stolen_models.show()

    print("\nTop 5 states with the most stolen cars:")
    top_stolen_states.show()

    print("\nMost common country of origin for car models purchased by Americans based on total theft :")
    sorted_country_counts1.limit(1).show()

    print("\nMost common country of origin for car models purchased by Americans based on car models :")
    sorted_country_counts2.limit(1).show()


    # Write results to CSV files
    top_stolen_models.write.mode("overwrite").csv(f"{output_path}/top_stolen_models")
    top_stolen_states.write.mode("overwrite").csv(f"{output_path}/top_stolen_states")
    sorted_country_counts1.write.mode("overwrite").csv(f"{output_path}/sorted_country_counts1")
    sorted_country_counts2.write.mode("overwrite").csv(f"{output_path}/sorted_country_counts2")


# Main Execution Block
if __name__ == '__main__':

    output_base_path3 = "taskfiles/Logging"
    create_or_clear_directory(output_base_path3)

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(output_base_path3+"/file.log", mode="w")  # Log to a file
        ]
    )
    logging.info("Starting the main execution block...")

    file_paths = ['taskfiles/cars.csv', 'taskfiles/Sheet1.csv', 'taskfiles/2015_State_Top10Report_wTotalThefts.csv']
    updated_file_path = 'taskfiles/Sheet1.csv'
    #api_url = 'http://127.0.0.1:8080/cars/getbyBrand'
    api_url = 'http://127.0.0.1:8080/server/car/getbyBrand/?brand='
    output_base_path1 = 'taskfiles/output_API'  # where each car model with its country
    output_base_path2 = 'taskfiles/output_updated'  # the result of updated data from sheet1
    output_base_path4 = 'taskfiles/output_sql_result'  # the result of updated data from sheet1

    create_or_clear_directory(output_base_path1)
    create_or_clear_directory(output_base_path2)
    create_or_clear_directory(output_base_path4)

    spark_session = start_spark_session()
    dfs = read_data_set_by_spark(spark_session, file_paths)
    df_spark_car, df_spark_sheet, df_sparkTotalThefts = dfs


    updated_dataset = update_dataset(df_sparkTotalThefts, df_spark_sheet)

    #for count query

    updated_dataset = updated_dataset.withColumn("Thefts", col("Thefts").cast("int"))

    updated_dataset.write.option("header", "true").mode("overwrite").csv(output_base_path2)

    df_carModel_Country = extract_car_model_and_origin(api_url, updated_dataset, output_base_path1)

    spark_sql_query(updated_dataset, df_carModel_Country, output_base_path4)

    logging.info("Closing down client-server connection")
    logging.info("end the main execution block...")

    # To See spark UI at http://localhost:4040
    #while True:
    #    pass

