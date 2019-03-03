import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{sum, _}
object Report {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HdfsTest")
      //      .config("spark.debug.maxToStringFields", 100)
      .config("spark.sql.shuffle.partitions",2)
      .master("local")/*服务器上不用*/
      .getOrCreate()

//    val schema = new StructType()
//      .add("database",StringType)
//      .add("es",StringType)
//      .add("id",StringType)
//      .add("isDdl",StringType)
//      .add("data",)

    /**
      * 	"database": "tybdata",
	"es": 1549331895000,
	"id": 26374,
	"isDdl": false,
      */
    val schema = new StructType()
      .add("database",StringType)
      .add("es",StringType)
      .add("id",IntegerType)
      .add("idDel",StringType)
      .add("data", new StructType()
        .add("usreId", IntegerType)
      .add("creatTime", StringType)
      .add("regTime", StringType)
      .add("isReg", IntegerType)
      .add("isFirstCharge", IntegerType)
      .add("firstChargeMoney", FloatType)
      .add("isFirstBetToday", IntegerType)
      .add("betAmount", FloatType)
      .add("validBetAmount", FloatType)
      .add("netMoney", FloatType)
      .add("isFirstChargeToday", IntegerType)
      .add("Charge", FloatType)
      .add("FloatType", FloatType)
      .add("OutMoney", FloatType)
      .add("api_name", StringType)
      .add("ip", StringType)
      .add("redBet", StringType)
      .add("backBet", StringType)
      .add("preferential", StringType)
      .add("agent", StringType)
      .add("field1", StringType)
      .add("field2", StringType)
      .add("field3", StringType)
      .add("field4", StringType)
      .add("field5", StringType)
      )




//    val df = spark.readStream.format("socket")
//      .option("host", "localhost")
//      .option("port", 9999)
//      .load()

    val df = spark.readStream.format("json")
      .schema(schema)
      .load("E:\\opt\\moudle\\game\\src\\main\\resources")
        .select("data")
    val s = df.writeStream.format("console").start()
    s.awaitTermination()

    /**
      * {"usreId":266,"creatTime":"2018-06-25 16:05:06","regTime":"2018-06-25 16:05:06","isReg":1,"isFirstCharge":0,"firstChargeMoney":0,"isFirstBetToday":0,"betAmount":0,"validBetAmount":0,"netMoney":0 ,"isFirstChargeToday":0,"Charge":0,"isOutMoneyToday":0,"OutMoney":0,"api_name":"AG","ip":"www.yabo.com","redBet":0,"backBet":0,"preferential":0,"agent":0,"field1":0,"field2":0,"field3":0,"field4":0,"field5":0}
      */
    import spark.implicits._







//    val df = data.as[String].map(s => s.split(",")).map(
//      d => ReportData(d(0),d(1),d(2),d(3),d(4),d(5),d(6),d(7),d(8),d(9),
//        d(10),d(11),d(12),d(13),d(14),d(15),d(16),d(17),d(18),d(19),
//        d(20),d(21),d(22),d(23),d(24)
//      )
//    )




//    val x = df.groupBy(df.col("creatTime").toString().split(" ")(0)).agg(
//      //注册数
//      sum(df.col("isReg")),
//      //首存人数
//      sum(df.col("isFirstCharge")),
//      //转换率
//      sum(df.col("isFirstCharge")).cast("double")/sum(df.col("isReg")),
//      //首存额
//      sum(df.col("firstChargeMoney")),
//      //人均首存
//      sum(df.col("firstChargeMoney"))/sum(df.col("isFirstCharge")),
//      //存款人数
//      sum(df.col("isFirstBetToday")),
//      //取款人数
//      sum(df.col("isOutMoneyToday")),
//      //存款额
//      sum(df.col("Charge")),
//      //取款额
//      sum(df.col("OutMoney")),
//      //存提差
//      sum(df.col("Charge"))-sum(df.col("OutMoney")),
//      //提存率
//      sum(df.col("OutMoney"))/sum(df.col("Charge")),
//      //投注人数
//      sum(df.col("isFirstBetToday")),
//      //有效投注额
//      sum(df.col("validBetAmount"))
//      //公司输赢
////      sum(df.col(""))
//    )
//
//    val query = x.writeStream.format("console").outputMode("complete").start()
//    query.awaitTermination()
  }

}
