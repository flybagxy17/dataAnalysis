import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test02 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)

    val x = lines.map(line =>{
          val arr = line.split(",")
          val creatTime = arr(0)
          val regTime = arr(1)
          val firstPunch = arr(2)
          val name = arr(3)
          var regNum = 0
          if (regTime.equalsIgnoreCase("null"))
            regNum
          else
            regNum =1

          ((creatTime),(regNum))
        }).reduceByKey(_+_)
x.print()
//    val words = lines.flatMap(_.split(" "))
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)
//    wordCounts.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination
  }


}
