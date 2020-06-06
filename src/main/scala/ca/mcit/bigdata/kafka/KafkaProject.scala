
/*
package ca.mcit.bigdata.kafka


package ca.mcit.bigdata.kafka

import java.util.Properties

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConverters._
import org.joda.time.DateTime

 object KafkaProject extends App {


  val property: Properties = new Properties()
  property.put("bootstrap.servers", "172.16.129.58:9092")
  property.put("group.id", "pradeep7")
  property.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer")
  property.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  property.put("auto.offset.reset",  "earliest")
  val consumer1 = new KafkaConsumer[String, String](property)

  val consumer2 = new KafkaConsumer[String, String](property)

  val producerProp: Properties = new Properties()
  producerProp.put("bootstrap.servers", "172.16.129.58:9092")
  producerProp.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
  //producerProp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProp.put("value.serializer",   "io.confluent.kafka.serializers.KafkaAvroSerializer")
  producerProp.put("schema.registry.url", "http://172.16.129.58:8081")
  //val producer = new KafkaProducer[String, String](producerProp)

  val producer = new KafkaProducer[String, GenericRecord](producerProp)

  consumer1.subscribe(List("summer2019_pradeep_trip","summer2019_pradeep_calendar","summer2019_pradeep_route","summer2019_pradeep_enriched_trip").asJava)

  //consumer1.subscribe(List("pradeep_rating_v1").asJava)

  while(true) {

    println("Batch start => " + new DateTime())

    val polledRecords: ConsumerRecords[String, String] = consumer1.poll(1000)
    polledRecords.forEach(consumerRecord => {
      println(consumerRecord.value())
      val tripFields: Array[String] =consumerRecord.value().split(",",-1)
      val trip = Trip
        .newBuilder()
        .setRouteId(tripFields(0).toInt)
        .setServiceId(tripFields(1))
        .build()

      val routeFields:Array[String] = consumerRecord.value().split(",",-1)
      val route=Route
        .newBuilder()
        .setRouteId(routeFields(0).toInt)
        .build()

      val calendarFields:Array[String] = consumerRecord.value().split(",",-1)
      val calendar=Calendar
        .newBuilder()

        .setServiceId(calendarFields(0))
        .build()



      //generate final message
      val enrichedTrip = EnrichedTrip
        .newBuilder()
        .setTrip(trip)
        .setCalendar(calendar)
        .setRoute(route)
        .build()

      println("Batch end => " + new DateTime())
println(enrichedTrip)


      val producedMessage = new ProducerRecord[String, GenericRecord](
        "summer2019_pradeep_enriched_trip",
        // "p_" + consumerRecord.value()) // transformation
        enrichedTrip)
      producer.send(producedMessage)
      println(producedMessage)
    })

    consumer1.commitSync()
    Thread.sleep(4000)
  }

}

/*
 while(true) {
    println("Batch start => " + new DateTime())
    val polledRecords: ConsumerRecords[String, String] = consumer.poll(1000)
    polledRecords.forEach(consumerRecord => {
      val producedMessage = new ProducerRecord[String, String](
        "iraj_movie_transformed",
        "p_" + consumerRecord.value()) // transformation
      producer.send(producedMessage)
    })
    consumer.commitSync()
    Thread.sleep(4000)666
  }
 */



*/