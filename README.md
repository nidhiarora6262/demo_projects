##demo_projects

ScalaVersion := "2.11.12"
Spark version :="2.4.0"
Scala Test    :="3.0.8"


Query 1 :To read the data through in built read function .
~~~
spark.read.option("header", "true").csv("/home/nidhi/Desktop/FL_insurance_sample.csv")
~~~
Query 2: To show the schema and data of file through in built functions.

~~~
data.printschema()
~~~
~~~
data.show()
~~~
Query 3 :Casting of columns into Doubletype.
~~~
data1.withColumn("eq_site_limit", data1("eq_site_limit").cast(DoubleType) .withColumn("hu_site_limit",data1("hu_site_limit").cast(DoubleType))
~~~
Query 4: Unpivot the data
~~~
data1.selectExpr("policyId", "statecode", "county","stack(3, 'eq_site_limit',eq_site_limit, 'hu_site_limit',hu_site_limit" as
as (LimitValue,Limitcode)"
~~~

Query 5 and query 6: To  count of limit_value for each policyID, statecode, county, limit_code and Add processing_datetime_utc column to output after step 5 and show results
~~~
DF4.groupBy("policyId", "statecode", "county", "Limitcode").agg(count("Limitvalue")).withColumn("processing_datetime_utc", lit((0))).show()
~~~
Query 7:Get the count of all records by grouping point_latitude and point_longitude on data retrieved.
~~~
val df7 = DF4.groupBy("point_longitude", "point_latitude").count().show()
~~~
Query 8: Check how many point_latitude and point_longitude values are there for each policyID.
~~~
 val df8 = data1.groupBy("policyID").agg(collect_list("point_longitude"), collect_list("point_latitude")).show()
~~~


 
