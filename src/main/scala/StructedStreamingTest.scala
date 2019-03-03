import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object StructedStreamingTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HdfsTest")
//      .config("spark.debug.maxToStringFields", 100)
        .config("spark.sql.shuffle.partitions",2)
      .master("local")/*服务器上不用*/
      .getOrCreate()







//    val lines = spark.readStream
//      .format("socket")
//      .option("host", "192.168.1.107")
//      .option("port", 9999)
//      .load()
//
//
//    import spark.implicits._
//    val words = lines.as[String].flatMap(_.split(" "))
//
//    // Generate running word count
//    val wordCounts = words.groupBy("value").count()
//    val query = wordCounts.writeStream
//      .outputMode("complete")
//      .format("console")
//      .start()



    val data = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    import spark.implicits._
//    val df = data.as[String].map(s =>s.split(",")).map( p => Person(
    //      p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9),
    //      p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19),
    //      p(20), p(21), p(22),p(23),p(24),p(25),p(26),p(27), p(28),p(29), p(30),
    //      p(31)))
    val df = data.as[String].map(s => s.split(",")).map(d => Data(
      d(0),d(1),d(2),d(3)
    ))
//    val person = df.as[Person]

    import org.apache.spark.sql.expressions.scalalang.typed
//    df.createOrReplaceTempView("person")
//    val x = df.selectExpr("select field1 from  person")

//    person.groupBy("")
//    val x = df.groupByKey(_.field9).agg(typed.sum(_.netAmount.toDouble))
//    val query = x.writeStream.format("console").outputMode("complete").start()
//    query.awaitTermination()

//    val query = df.filter(df.col("registerTime").=!=("null")).writeStream.format("console").start()
//    val y = df.filter(df.col("registerTime").=!=("null")).col("registerTime")
    df.createOrReplaceTempView("rest")
    val x = spark.sql("select count(registerTime) as rt from rest group by date ")
//    df.groupByKey(x => x.date)
//    val x = df.groupBy(df.col("date").as("date")).
//      agg(
//        count(df.filter(df.col("registerTime").=!=("null")).col("registerTime"))

//        count(df.filter(df.col("registerTime").isNotNull and(df.col("registerTime").<=>(""))).col("registerTime")).as("registerNum"),
//        count(df.filter(df.col("registerTime").isNotNull and(df.col("registerTime").<=>(""))and (df.col("c").isNotNull)).col("registerTime").as("firstRegisterPunch"))
//        (df.filter(df.col("registerTime").isNotNull and(df.col("registerTime").<=>(""))and (df.col("firstPunch").isNotNull)).col("registerTime")/df.filter(df.col("registerTime").isNotNull and(df.col("registerTime").<=>("")).as("time")).col("registerTime")) as("percentConversion")
//      )


    df.select()
        val query = x.writeStream.format("console").outputMode("complete").start()
        query.awaitTermination()
  }

}
