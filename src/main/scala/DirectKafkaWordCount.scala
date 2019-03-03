import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectKafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    //    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Get the lines, split them into words, count the words and print
//    val lines = messages.map(_.value)

    messages.foreachRDD(rdd => {
      val x = rdd.foreachPartition(partition => {
        partition.foreach(tuple => {
          val line = tuple.value()
          val arr = line.split(",")
          val id = arr(0)
          val creatTime = arr(1)
          val regTime = arr(2)
          val firstChargeTime = arr(3)
          val firstChargeMoney = arr(4)
          val betAmount = arr(5)
          val validBetAmount = arr(6)
          val netAmount = arr(7)
          val Charge = arr(8)
          val OutMoney = arr(9)
          val api_name = arr(10)
          val ip = arr(11)
          val redBet = arr(12)
          val backBet = arr(13)
          val preferential = arr(14)
          val agent = arr(15)
          ((creatTime), (id, creatTime, regTime, firstChargeTime))
        })

      }
      )
    })
    /**
      * "id":265,"creatTime":"2018-06-23 16:05:06","regTime":"2018-06-23 16:05:06","firstChargeTime":null,"firstChargeMoney":0,"betAmount":0,"validBetAmount":0,"netAmount":0,"Charge":0,"OutMoney":0,"api_name":"AG","ip":"www.yabo.com","redBet":0,"backBet":0,"preferential":0,"agent":0,"field1":null,"field2":null,"field3":null,"field4":null,"field5":null
      */
    messages.foreachRDD(
      rdd => rdd.foreachPartition(partition=>{
      partition.foreach(tuple =>{
        val line = tuple.value()
        val arr = line.split(",")
        val id = arr(0)
        val creatTime = arr(1)
        val regTime = arr(2)
        val firstChargeTime = arr(3)
        val firstChargeMoney = arr(4)
        val betAmount = arr(5)
        val validBetAmount = arr(6)
        val netAmount = arr(7)
        val Charge = arr(8)
        val OutMoney = arr(9)
        val api_name = arr(10)
        val ip = arr(11)
        val redBet = arr(12)
        val backBet = arr(13)
        val preferential = arr(14)
        val agent = arr(15)
        ((creatTime),(id,creatTime,regTime,firstChargeTime))
      })

    }
    ))




    messages.map(line =>{
      val arr = line.value().split(",")
      val id = arr(0)
      val creatTime = arr(1)
      val regTime = arr(2)
      val firstChargeTime = arr(3)
      val firstChargeMoney = arr(4)
      val betAmount = arr(5)
      val validBetAmount = arr(6)
      val netAmount = arr(7)
      val Charge = arr(8)
      val OutMoney = arr(9)
      val api_name = arr(10)
      val ip = arr(11)
      val redBet = arr(12)
      val backBet = arr(13)
      val preferential = arr(14)
      val agent = arr(15)
      if (creatTime.equalsIgnoreCase("null"))
        0
      else
        1
      ((creatTime),(id,creatTime,regTime,firstChargeTime))
    }).groupByKey()

//
//    val words = lines.flatMap(_.split(" "))
////    words.transform(x =>x.groupBy(f:String =""))
//    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
//    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}