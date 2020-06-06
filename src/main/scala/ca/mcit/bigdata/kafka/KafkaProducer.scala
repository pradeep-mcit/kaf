package ca.mcit.bigdata.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
object KafkaProducer extends App {

/*  val tripList: List[Trip] = Source
    .fromFile("/home/bd-user/Documents/trips.txt")
    .getLines()
    .toList
    .tail
    .map(Trip(_))

  val routeList: List[Route] = Source
    .fromFile("/home/bd-user/Documents/routes.txt")
    .getLines()
    .toList
    .tail
    .map(Route(_))

  val routeTrip: List[JoinOutput] = new GenericMapJoin[Trip, Route]((i) => i.route_id.toString)((j) => j.route_id.toString)
    .join(tripList, routeList)

  val calendarList: List[Calendar] = Source.fromFile("/home/bd-user/Documents/calendar.txt")
    .getLines()
    .toList
    .tail // a collection of lines-
    .map(Calendar(_))
  //val tripList: Iterator[String] = Source.fromFile("/home/bd-user/Downloads/test.txt").getLines()

  val enrichedTrip = new GenericNestedLoopJoin[Calendar, JoinOutput]((i, j) => i.service_id == j.left.asInstanceOf[Trip].service_id)
    .join(calendarList,routeTrip) */
val calendarList: List[pra] = Source.fromFile("/home/bd-user/Downloads/test.csv")
  .getLines()
  .toList
  .tail // a collection of lines-
  .map(pra(_))

  val props: Properties = new Properties()
  props.put("bootstrap.servers", "172.16.129.58:9092")
  props.put("ConsumerConfig.FETCH_MAX_BYTES_CONFIG", "9997750")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")

  val producer = new KafkaProducer[String, String](props)
  val topic = "varun_topic"

  try {
    var  i = 0
    for  (line <- calendarList) {
      val record = new ProducerRecord[String,String]("varun_topic", "record_" + i, line.toString)
      val metadata = producer.send(record)
      printf(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(), metadata.get().partition(),
        metadata.get().offset())
      i+=1
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}


