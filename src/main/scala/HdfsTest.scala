/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
import org.apache.spark.sql.SparkSession


object HdfsTest {


  /**
    * 数据集格式
    * 217218853,"0","B20181001000003BY1PD",35,"ars699","ars699",62174,-1.20,"2018-10-01 00:00:00","3",1.20,1.20,1,"",0,"","",\N,0.00,"2018-10-01 00:00:03",\N,"PG","57295158-57295158-201-0","",0,0,"美杜莎 2","","",0.00,0,"2018-10-01 00:00:00"
    */

  /**
    * 字段说明
    * 第六个：name '账号', 第八个：netAmount '额度', 第十一个：betAmount '金额', 第十二个：validBetAmount '有效',
    */
  /** Usage: HdfsTest [file] */
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("HdfsTest")
      .master("local")/*服务器上不用*/
      .getOrCreate()
//    val file = spark.read.text(args(0)).rdd

    val data = spark.sparkContext.textFile("file:///E:\\opt\\moudle\\game\\src\\main\\resources\\test.txt")
    import spark.implicits._
    val df = data.map(s =>s.split(",")).map(p => Person(
        p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9),
        p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19),
        p(20), p(21), p(22),p(23),p(24),p(25),p(26),p(27), p(28),p(29), p(30),
        p(31))).toDF()




    df.createOrReplaceTempView("person")
//    val df1 = spark.sql("select * from person ")

//    df1.show()

    /**
      *
      *1.  3.每个用户的输赢
      */

    /**
      * 1.用Spark计算投注前十名的用户
      */
    val df2 = spark.sql("select name,sum(validBetAmount) as validBetAmountSum  from person group by name order by validBetAmountSum desc limit 10")
    df2.show()


    /**
      * 2.每个用户的有效投注
      */

    val df3 = spark.sql("select name,sum(validBetAmount) as validBetAmountSum  from person group by name")
    df3.show()

    /**
      * 3.每个用户的输赢
      */
    val df4 = spark.sql("select name,sum(netAmount) as netAmountSum  from person group by name ")
    df4.show()



    spark.stop()
  }
}

