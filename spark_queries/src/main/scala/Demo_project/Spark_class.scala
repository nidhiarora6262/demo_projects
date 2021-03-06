package Demo_project


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
class Spark {
  
  val spark = SparkSession.builder ().master ("local[*]").appName ("example of SparkSession")
  .config ("spark.some.config.option", "some-value")
  .getOrCreate ()
 
  // query 1 read the csv 
  val data = spark.read.option ("header", "true").csv ("/home/nidhi/Desktop/nidhiarora/spark_queries/src/main/resources/FL_insurance_sample.csv")
  //query 2 print the schema of file
  data.printSchema ()
  data.persist()
  //query 2 show the data of the file
  data.show ()
  
  
  
//query 3 casting of columns into Doubletype
  def schema1(eq_site_limit: String, hu_site_limit: String, fl_site_limit: String, fr_site_limit:String, tiv_2011:String, tiv_2012:String, eq_site_deductible:String,
              hu_site_deductible:String, fl_site_deductible:String, fr_site_deductible:String   ): DataFrame= {
    val  data2 = data.withColumn("eq_site_limit", col("eq_site_limit").cast(DoubleType))
      .withColumn("hu_site_limit", col("hu_site_limit").cast(DoubleType))
      .withColumn("fl_site_limit", col("fl_site_limit").cast(DoubleType))
      .withColumn("fr_site_limit", col("fr_site_limit").cast(DoubleType))
      .withColumn("tiv_2011", col("tiv_2011").cast(DoubleType))
      .withColumn("tiv_2012", col("tiv_2012").cast(DoubleType))
      .withColumn("eq_site_deductible", col("eq_site_deductible").cast(DoubleType))
      .withColumn("hu_site_deductible", col("hu_site_deductible").cast(DoubleType))
      .withColumn("fl_site_deductible", col("fl_site_deductible").cast(DoubleType))
      .withColumn("fr_site_deductible", col("fr_site_deductible").cast(DoubleType))
    data2.persist()
  }
  
  //query 4 unpivot the data
  def unpivot( policyId:String,statecode:String, county:String, construction:String,point_latitude:String, point_longitude:String,
    eq_site_limit:Double, hu_site_limit:Double, fl_site_limit: Double, fr_site_limit:Double, tiv_2011:Double, tiv_2012:Double, eq_site_deductible:Double
    ,hu_site_deductible:Double, fl_site_deductible:Double, fr_site_deductible:Double ) :DataFrame= {
    val DF4 = data.selectExpr("policyId", "statecode", "county", "construction", "point_latitude",
      "point_longitude", "stack(10, 'eq_site_limit',eq_site_limit, 'hu_site_limit',hu_site_limit, 'fl_site_limit',fl_site_limit," +
        " 'fr_site_limit',fr_site_limit,'tiv_2011',tiv_2011,' tiv_2012',tiv_2012, 'eq_site_deductible',eq_site_deductible,' hu_site_deductible', hu_site_deductible," +
        "'fl_site_deductible',fl_site_deductible, 'fr_site_deductible', fr_site_deductible) as (LimitValue,Limitcode)")

    DF4
    
    
     //query 5 count of limit_value for each policyID, statecode, county, limit_code and Add processing_datetime_utc column to output after step 5 and show results
    val df3 = DF4.groupBy("policyId", "statecode", "county", "Limitcode").agg(count("Limitvalue")).withColumn("processing_datetime_utc", lit((0)))
    
      // query 8 Check how many point_latitude and point_longitude values are there for each policyID
    val df7 = DF4.groupBy("point_longitude", "point_latitude").count()
    DF4
  }
  //Check how many point_latitude and point_longitude values are there for each policyID
  def groupby (policyId:String,  point_latitude:String, point_longitude:String):DataFrame = {

    val df8 = data.groupBy("policyID").agg(collect_list("point_longitude"), collect_list("point_latitude"))

    df8


  }
}

object Spark_Class extends Spark
{
  def main (args:Array[String]):Unit ={
        val obj = new Spark()

  spark.time(obj.schema1 ("498960", "498960", "49896","498960",  "498960",  "792148.9", "0,9979.2","0","7890","0").printSchema())    
   spark.time( obj.unpivot (" 119736", "FL","CLAY COUNTY","Masonry","30.102261","-81.711777",  498960, 498960, 49896,498960,  498960,792148.9, 0,9979.2,0,0).show())
  spark.time(obj.groupby ("100735","[-82.461929]","[29.048983]").show())

}
}















