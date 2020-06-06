package ca.mcit.bigdata.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
object KafkaConsumer extends App {

  val props:Properties = new Properties()
  props.put("group.id", "ss")
  props.put("bootstrap.servers","172.16.129.58:9092")
  props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
  val topics = List("bixi_test")
  try {
    consumer.subscribe(topics.asJava)
    //consumer.subscribe(Collections.singletonList("topic_partition"))
    //consumer.subscribe(Pattern.compile("topic_partition"))
    while (true) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        println("Topic: " + record.topic() + ", Key: " + record.key() + ", Value: " + record.value() +
          ", Offset: " + record.offset() + ", Partition: " + record.partition())
      }
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
  }
}


