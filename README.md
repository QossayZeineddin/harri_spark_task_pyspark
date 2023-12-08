# harri_spark_task_pyspark

# Car Theft Analysis

## Task Details

This repository contains datasets representing car thefts in the US, with each row indicating the number of thefts for a specific car model. Additionally, there is a dataset containing information about car manufacturers and their countries of origin.

The task is divided into two parts: API and Spark.

### API Part

Build a Spring Boot application to create an API for searching the country of origin for cars. The API should be capable of paginated searches, with the page size determined by the client.
URL for the application : https://github.com/QossayZeineddin/harri_api_task.git
when yoy run the server we have multy get requset to get the data
1- Get the counter of origin for a car name  (for example http://127.0.0.1:8080/cars/getbyBrand/bmw)
2- Get the car brands of country (for example http://127.0.0.1:8080/cars/getby/Japan)
3- To get a specific numbers of car models (using limit http://127.0.0.1:8080/cars/getby/Japan/2   this get the first 2 cars in japan)
4- To get all cars (http://127.0.0.1:8080/cars/getAll)

## Database Configuration

This project utilizes MariaDB to create and manage the database. If you prefer a different SQL server, you can make the necessary adjustments by following these steps:

#### 1. Update Pom.xml and application.properties

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
spring.datasource.driver-class-name=org.mariadb.jdbc.Driver

### Database Tables

![Screenshot from 2023-12-08 14-23-53](https://github.com/QossayZeineddin/harri_spark_task_pyspark/assets/103140839/2a81db44-8c2f-49be-8d19-32c1528c70dc)
attae


### Spark Part

Build a Spark application to extract results from the provided datasets.

1. **Read Dataset and Extract Car Model and Country of Origin**
   - Read the dataset and extract a file containing the car model and its country of origin, utilizing the API built in the previous step.
   - Enhance the solution to generate a file for each country. Expect the input dataset to potentially exceed 100 GB, so parallelism is crucial.
   - Optimize performance using proper caching.

2. **Update Records**
   - Read a file with updated records and merge them with the original dataset. Consider the key for your dataset to be a combination of all columns except the rank column.

3. **Analysis Using SQL**
   - List the top 5 stolen car models in the U.S.
   - List the top 5 states based on the number of stolen cars.
   - Determine the most common country of origin for car models purchased by Americans, using SQL syntax.

## Usage

To execute the tasks, follow the instructions below:

1. **API Part**
   - Build and run the Spring Boot application.
   - Utilize the API for searching the country of origin for cars.

2. **Spark Part**
   - Execute the Spark application to perform data extraction and analysis.

## Project Structure

- `/api`: Contains the Spring Boot application for the API.
- `/spark`: Contains the Spark application for data extraction and analysis.

## Dependencies

Ensure you have the following dependencies installed:

- [Java](https://www.java.com/en/download/)
- [Spring Boot](https://spring.io/projects/spring-boot)
- [Apache Spark](https://spark.apache.org/)

## License

This project is licensed under the [MIT License](LICENSE).



What is spark and how it work : https://www.youtube.com/watch?v=IyHrVZ2uJkM&t=281s
spark book : https://github.com/gigamailer/simplenin3/blob/master/Spark%20in%20Action-Manning%25282016%2529.pdf
PySpark Tutorial  : https://www.youtube.com/watch?v=_C8kWso4ne4&t=939s

