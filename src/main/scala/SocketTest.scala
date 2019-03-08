import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object SocketTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HdfsTest")
      //      .config("spark.debug.maxToStringFields", 100)
      .config("spark.sql.shuffle.partitions",2)
      .master("local")/*服务器上不用*/
      .getOrCreate()

    val data = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

//    val df = data.as[ReportData]
//    val df = data.as[String].map(s => s.split(",")).map(d => ReportData(
//      d(0),d(1),d(2),d(3),d(4),d(5),d(6),d(7),d(8),d(9),
//      d(10),d(11),d(12),d(13),d(14),d(15),d(16),d(17),d(18),d(19),
//      d(20),d(21),d(22),d(23),d(24)
//    ))
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
//      sum(df.col("validBetAmount")),
//      //公司输赢
//      sum(df.col("netMoney")*(-1)),
//      //盈余比例
//      sum(df.col("netMoney")*(-1))/sum(df.col("validBetAmount")),
//      //红利
//      sum(df.col("redBet")),
//      //返水
//      sum(df.col("backBet")),
//      //存款优惠
//      sum(df.col("preferential")),
//      //公司收入
//      sum(df.col("netMoney")*(-1))-sum(df.col("redBet"))-sum(df.col("backBet"))-sum(df.col("preferential"))
//    )
//
//    val query = x.writeStream.format("console").outputMode("complete").start()
//    query.awaitTermination()
  }


}
