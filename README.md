###demo_projects






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
 
