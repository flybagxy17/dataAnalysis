package kafka_producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class GameDataProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //kafka 集群配置
        props.put("bootstrap.servers", "tstkj001:6667,tstkj002:6667,tstkj003:6667");
        props.put("acks", "1");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //用户 topic 和数据 自己模拟
        ProducerRecord<String, String> msg1 = new ProducerRecord<>("user", "");
        //游戏 topic 和数据 自己模拟
        ProducerRecord<String, String> msg2 = new ProducerRecord<>("game", "");

        send(producer,msg1);
        send(producer,msg2);

    }
    public static void send(KafkaProducer producer,  ProducerRecord<String, String> msg){
        producer.send(msg, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e !=null){
                    e.printStackTrace();
                }
            }
        });
    }
}
