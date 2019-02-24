/**
  * 测试类
  */
object WordCountTest {
  def main(args: Array[String]): Unit = {
     val params = new Array[String](2)
    //对应的是DirectKafkaWordCount中args[0] 的参数 kafka集群
      params(0) = "tstkj001:6667,tstkj002:6667,tstkj003:6667"
    //对应的是DirectKafkaWordCount中args[1] 的参数 topic
      params(1) = "user,game"
    DirectKafkaWordCount.main(params)
  }
}
