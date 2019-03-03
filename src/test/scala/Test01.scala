import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{count, _}
object Test01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HdfsTest")
      .config("spark.sql.shuffle.partitions",2)
      .master("local")/*服务器上不用*/
      .getOrCreate()
    val data = spark.read.textFile("file:///E:\\opt\\moudle\\game\\src\\main\\resources\\data.txt")
    import spark.implicits._
    val df = data.as[String].map(s => s.split(",")).map(d => Data(
      d(0),d(1),d(2),d(3)
    ))
    df.show()


    val df1  =df.filter(df.col("registerTime").=!=("null"))

    val accum = spark.sparkContext.longAccumulator("My Accumulator")
//    df.withColumn("df",col())

//    spark.sqlContext.udf.register("kk",getNo())
//    df.groupBy("").agg(sum(df.col("registerTime")),count())

      df.groupBy()
//        count(df.filter(df.col("registerTime").=!=("null")and (df.col("firstPunch").=!=("null"))).col("registerTime").as("firstRegisterPunch"))
        //        (df.filter(df.col("registerTime").isNotNull and(df.col("registerTime").<=>(""))and (df.col("firstPunch").isNotNull)).col("registerTime")/df.filter(df.col("registerTime").isNotNull and(df.col("registerTime").<=>("")).as("time")).col("registerTime")) as("percentConversion"
//    x.show()

  }
  def getNo():UserDefinedFunction ={

    val no = udf((t1:String,t2:String)=>{
      var result:Long =0
      if (t1.split(" ")(0).equalsIgnoreCase(t2.split(" ")(0)))
        result = 1L
      else
        result = 0L

      result
    })
    no
    }


}
