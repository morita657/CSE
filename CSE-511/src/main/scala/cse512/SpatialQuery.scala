package cse512
import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>
      {
        val rectangle = queryRectangle.split(',').map(_.toDouble)
        val point = pointString.split(',').map(_.toDouble)
        if ((rectangle(0)-point(0))*(rectangle(2)-point(0))<=0 
        && (rectangle(1)-point(1))*(rectangle(3)-point(1))<=0)
          true
        else
          false
      }
    )

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")
    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>
      {
        val rectangle = queryRectangle.split(',').map(_.toDouble)// given data
        val point = pointString.split(',').map(_.toDouble)//specific locations
        if ((rectangle(0)-point(0))*(rectangle(2)-point(0))<=0 && (rectangle(1)-point(1))*(rectangle(3)-point(1))<=0)
          true
        else
          false
      }
    )

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>
      {
        val point1 = pointString1.split(',').map(_.toDouble)
        val point2 = pointString2.split(',').map(_.toDouble)
        val calculatedDistance = math.sqrt(math.pow((point1(0) - point2(0)), 2) + math.pow((point1(1) - point2(1)), 2))
        if (calculatedDistance <= distance)
          true
        else
          false
      }
    )

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")
    pointDf2.show()

    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>
      {
        val point1 = pointString1.split(',').map(_.toDouble)
        val point2 = pointString2.split(',').map(_.toDouble)
        val calculatedDistance = math.sqrt(math.pow((point1(0) - point2(0)), 2) + math.pow((point1(1) - point2(1)), 2))
        if (calculatedDistance <= distance)
          true
        else
          false
      }
    )

    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")




    //resultDf.show(false)

    return resultDf.count()
  }
}