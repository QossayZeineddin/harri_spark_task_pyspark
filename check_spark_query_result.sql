use harri_task_API;


select tr.make_model  , sum(tr.thefts) as total from top10report tr 
group by tr.make_model 
ORDER BY total DESC 
LIMIT 5;

select tr.state  , sum(tr.thefts) as total from top10report tr 
group by tr.state  
ORDER BY total DESC 
LIMIT 5;


# most country  buyed in the usa beasd on car model
SELECT mo.Country_of_Origin, COUNT(DISTINCT tr.Make_Model) AS Distinct_Make_Models
FROM top10report tr
INNER JOIN model_origin mo ON mo.Make_Model = tr.Make_Model
GROUP BY mo.Country_of_Origin
ORDER BY Distinct_Make_Models DESC;



# most country  buyed in the usa beasd on totasl car models theft and its country

SELECT
    mo.Country_of_Origin,
    SUM(CAST(tr.Thefts AS SIGNED)) AS TotalThefts,
    COUNT(*) AS car_models_count_with_out_distinct
FROM top10report  tr
JOIN model_origin mo ON tr.Make_Model = mo.Make_Model
GROUP BY mo.Country_of_Origin
ORDER BY TotalThefts DESC;




select * from cars c where c.country_of_origin  = "America";

select COUNT (*) from top10report tr where tr.make_model like "%Chevrolet%" or 
tr.make_model like "%Chrysler%" or
tr.make_model like "%Dodge%" or 
tr.make_model like "%Ford%" or 
tr.make_model like "%Jeep%" or
tr.make_model like "%Tesla%" or
tr.make_model like "%Cadillac%";




select COUNT (*) from top10report tr where 
tr.make_model like "%Daihatsu%" or 
tr.make_model like "%Datsun%" or
tr.make_model like "%Honda%" or 
tr.make_model like "%Infiniti%" or 
tr.make_model like "%Isuzu%" or
tr.make_model like "%Tesla%" or
tr.make_model like "%Lexus%" or
tr.make_model like "%Mazda%" or 
tr.make_model like "%Mitsubishi%" or
tr.make_model like "%Nissan%" or
tr.make_model like "%Subaru%" or
tr.make_model like "%Nissan%" or
tr.make_model like "%Suzuki%" or
tr.make_model like "%Toyota%" 
;

select * from cars c where c.country_of_origin  = "Japan";





select COUNT (*) from top10report tr where 
tr.make_model like "%Aston Martin%" or 
tr.make_model like "%Bentley%" or
tr.make_model like "%Caterham%" or 
tr.make_model like "%Jaguar%" or 
tr.make_model like "%Land Rover%" or
tr.make_model like "%Lotus%" or
tr.make_model like "%McLaren%" or
tr.make_model like "%MG%" or 
tr.make_model like "%Mini%" or
tr.make_model like "%Rolls Royce%" 
;

select * from cars c where c.country_of_origin  = "England";



select COUNT (*) from top10report tr where 
tr.make_model like "%Audi%" or 
tr.make_model like "%BMW%" or
tr.make_model like "%Mercedes-Benz%" or 
tr.make_model like "%Opel%" or 
tr.make_model like "%Porsche%" or
tr.make_model like "%Smart%" or
tr.make_model like "%Volkswagen%" ;

select * from cars c where c.country_of_origin  = "Germany";



select COUNT (*) from top10report tr where 
tr.make_model like "%Abarth%" or 
tr.make_model like "%Alfa Romeo%" or
tr.make_model like "%Ferrari%" or 
tr.make_model like "%Fiat%" or 
tr.make_model like "%Lamborghini%" or
tr.make_model like "%Maserati%" ;

select * from cars c where c.country_of_origin  = "Italy";




select COUNT (*) from top10report tr where 
tr.make_model like "%Daewoo%" or 
tr.make_model like "%Hyundai%" or
tr.make_model like "%Kia%" or 
tr.make_model like "%Fiat%" or 
tr.make_model like "%SsangYong%";

select * from cars c where c.country_of_origin  = "South Korea";



