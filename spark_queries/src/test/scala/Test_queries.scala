import Demo_project.Spark_Class
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

//import Demo_project.spark_queries
//import org.testng.Assert._


class Test_queries extends FunSuite {
  var spark: SparkSession = _

  //val data1 = spark.read.option("header", "true").csv("/home/nidhi/Documents/datanew.csv")
  def beforeEach() {
    spark = new SparkSession.Builder().appName("Spark Job for Loading Data").master("local[*]").getOrCreate()
  }

  /*test("testing RDD") {
    val data1 = spark.read.option("header", "true").csv("/home/nidhi/Desktop/nidhiarora/spark_queries/src/main/resources/FL_insurance_sample.csv")
    assert(data1.count() != 0)
  }*/




//  test("casting") {
//
//    val abc = Spark_Class.schema1("498960", "498960", "49896","498960",  "498960",  "792148.9", "0,9979","0","7890","0")
//    assert ( data.schema === abc.schema)
//  }

  test("groupBY") {

    val ans = Spark_Class.groupby("100735","[-82.461929]","[29.048983]")
    assert(ans.count > 1)

  }


  test("unpivot") {
    val ans1 = spark_queries.unpivot(
    val query4 = Spark_Class.unpivot("119736" , "FL","CLAY COUNTY" , "Masonry" ," 30.102261","-81.711777",498960, 498960, 49896,498960,498960,  792148.9, 0,9979.2,0,0)
    assert(ans1.collect() === 5)

  }
}


