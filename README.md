# Harri spring boot and spark Task

# Car Theft Analysis

## Task Details

This repository contains datasets representing car thefts in the US, with each row indicating the number of thefts for a specific car model. Additionally, there is a dataset containing information about car manufacturers and their countries of origin.

The task is divided into two parts: API and Spark.

### API Part

Build a Spring Boot application to create an API for searching the country of origin for cars. The API should be capable of paginated searches, with the page size determined by the client.  
GitHub repo [Spring Boot application](https://github.com/QossayZeineddin/harri_api_task.git)  
URL for the application : https://github.com/QossayZeineddin/harri_api_task.git  
when yoy run the server we have multy get requset to get the data  
1- Get the country of origin for a car name  (http://127.0.0.1:8080/server/car/getbyBrand/?brand=bmw)  

<img src="https://github.com/QossayZeineddin/harri_spark_task_pyspark/assets/103140839/381717e2-c239-4d6f-8c85-010d0444f931" width="210" height="270" alt="Screenshot from 2023-12-08 16-47-07">


2- Get the car brands of country (for example http://127.0.0.1:8080/cars/getby/Japan)  

<img src="https://github.com/QossayZeineddin/harri_spark_task_pyspark/assets/103140839/0cf33d0f-9d8c-4435-b095-d7ade0fce5a9" width="210" height="270" alt="Screenshot from 2023-12-08 16-47-38">



3- To get a specific numbers of car models (using limit http://127.0.0.1:8080/cars/getby/Japan/2   this get the first 2 cars in japan)  
4- To get all cars (http://127.0.0.1:8080/cars/getAll)  

#### Database Configuration

This project utilizes MariaDB to create and manage the database. If you prefer a different SQL server, you can make the necessary adjustments by following these steps:

####  Update Pom.xml and application.properties

Navigate to the `pom.xml` file and modify the database dependency to match your preferred SQL server. Replace the existing dependency with the appropriate one for your chosen database.

```xml
<!-- Replace the following dependency with the appropriate one for your SQL server -->
<dependency>
    <groupId>org.mariadb.jdbc</groupId>
    <artifactId>mariadb-java-client</artifactId>
    <version>3.0.9</version>
</dependency>
```
Open the application.properties file and adjust the database driver to match the driver class for your SQL server. Replace the existing driver class with the one required for your chosen SQL server.
 Replace the following driver class with the appropriate one for your SQL server and change the database root name and sql password 
```
spring.datasource.driver-class-name=org.mariadb.jdbc.Driver
spring.datasource.username=admin
spring.datasource.url=jdbc:mariadb://localhost:3306/harri_task_API

```
### Database Tables

![Screenshot from 2023-12-08 14-23-53](https://github.com/QossayZeineddin/harri_spark_task_pyspark/assets/103140839/2a81db44-8c2f-49be-8d19-32c1528c70dc)



## Spark Part

Build a Spark application to extract results from the provided datasets.  
 - Initially, I considered using Spark RDDs for this Task . However, due to the complexity , time limit and after conducting research, I opted to leverage the more streamlined and optimized approach provided by Spark DataFrames (in Task.py).-  


1. **Read Dataset and Extract Car Model and Country of Origin**
   - Read the dataset and extract a files containing the car model and its country of origin, utilizing the API built in the previous step.
     'In this step, the Spark application reads the provided datasets, extracting information about car models and their corresponding countries of origin. The process is optimized         using the Spark DataFrame API for efficient and distributed data processing.'
      ```
     # Initialize the Spark session
        spark_session = start_spark_session()

        # Read the dataset into Spark DataFrames
        dfs = read_data_set_by_spark(spark_session, file_paths)
        # Other code...

        # Extract car model and country of origin
        df_carModel_Country = extract_car_model_and_origin(api_url, updated_dataset, output_base_path1)
     ```

        The code utilizes functions such as start_spark_session to initialize the Spark session and read_data_set_by_spark to read the dataset files (CSV format) into Spark                 DataFrames.     The main extraction is performed by the extract_car_model_and_origin function, which uses the Spark DataFrame API to select distinct car models and determine         their respective         countries of origin through API calls. The results are stored in separate files for each country within the specified output path.

   - Optimize performance using proper caching.
       Implement caching mechanisms, both at the Spark RDD and DataFrame levels, to store intermediate results that can be reused across multiple operations. This can significantly         reduce the need to recalculate certain values.
     ```
         car_models_with_origin.cache()

     ```
     this step decreasing the run time of the appleaction around 15 sec

    - Data Partitioning
      To enhance the efficiency of Spark transformations, a thoughtful data partitioning strategy has been implemented. The objective is to distribute the data evenly across nodes, preventing resource bottlenecks and ensuring a balanced workload, Then write in files for each country it's car models . The following step outline the data partitioning process:
      ```
          car_models_with_origin = car_models_with_origin.repartition('Country_of_Origin')
        countries = [row.Country_of_Origin for row in car_models_with_origin.select('Country_of_Origin').distinct().collect()]
    
        def write_partition_to_file(country):
            country_df = car_models_with_origin.filter(col('Country_of_Origin') == country)
            file_path = f'{output_base_path}/country_{country}.csv'
            country_df.drop('Country_of_Origin').write.mode('overwrite').csv(file_path)
    
        for country in countries:
            write_partition_to_file(country)


2. **Update Records**
   - To update records in the dataset, a comprehensive method named update_dataset has been implemented. This method seamlessly merges an original DataFrame with another DataFrame     containing updated records.
    Method Explanation:
    
        1- Define Key Columns:
            Identify key columns for uniquely identifying records. In this case, exclude columns like 'Rank' and 'Thefts.'
    
        2- Create Composite Key:
            Generate a composite key using the identified key columns for both the original and updated DataFrames.
    
        3- Rename Columns:
            Rename columns in the updated DataFrame, excluding the composite key, to avoid conflicts during the join.
    
        4- Full Outer Join:
            Perform a full outer join on the composite key to merge the original and updated DataFrames.
    
        5- Coalesce Columns:
            Prioritize updated data where available; otherwise, use the original data for each column.
    
        6- Drop Unnecessary Columns:
            Remove the composite key and columns from the updated DataFrame, leaving only the updated records.
    
    The update_dataset method ensures a comprehensive and efficient update of records in the dataset, utilizing Spark DataFrame operations for optimal performance.
    

3. **Analysis Using SQL**   
     For the analysis step, a method named spark_sql_query has been developed to leverage Spark SQL for querying and extracting meaningful insights from the datasets. The method          performs the following analyses:
   - List the top 5 stolen car models in the U.S. 
   - List the top 5 states based on the number of stolen cars.
   - Determine the most common country of origin for car models purchased by Americans, using SQL syntax.
  
       Method Explanation:
    
        Top 5 Stolen Car Models:
            Utilize Spark SQL syntax to group the DataFrame by "Make_Model," calculate the total thefts, rename the column, order by the total thefts in descending order, and limit to             the top 5 results.
    
        Top 5 States with the Most Stolen Cars:
            Similar to the first analysis, group the DataFrame by "State," calculate the total thefts, rename the column, order by the total thefts in descending order, and limit to the         top 5 results.
    
        Based on the models, what is the most country from where Americans buy their cars?:
            based on the data that we have there are two Solution first take the total theft of each
             country cars and the second just count car models and its country that the american have
            Join the two DataFrames (df_report and df_carModel_Country) on the common column 'Make_Model.'
            Group the resulting DataFrame by 'Country_of_Origin' and count the total theft cars of each country secand solution count total models of each country .
            Sort the results in descending order based on the count.
    
        Display Results:
            Display the results for each analysis using the show() method.

## Usage

To execute the tasks, follow the instructions below:

1. **API Part**
   - Build and run the Spring Boot application.
   - Utilize the API for searching the country of origin for cars.

2. **Spark Part**
   - Execute the Spark application to perform data extraction and analysis. - Use Task.py to run the pyspark application -

## resources

   - What is spark and how it work : https://www.youtube.com/watch?v=IyHrVZ2uJkM&t=281s  
   - spark book : https://github.com/gigamailer/simplenin3/blob/master/Spark%20in%20Action-Manning%25282016%2529.pdf  
   - PySpark Tutorial  : https://www.youtube.com/watch?v=_C8kWso4ne4&t=939s  

#######################################################   
the run of task.py console logs  

![Screenshot from 2023-12-08 21-45-50](https://github.com/QossayZeineddin/harri_spark_task_pyspark/assets/103140839/a5a904c6-8ac8-44c7-95b2-42c914276bda)


