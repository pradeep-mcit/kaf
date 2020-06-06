package ca.mcit.bigdata.kafka

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
object SchemaPractice extends App {

  println(Movie.SCHEMA$.toString(true))

  println(EnrichedMovie.SCHEMA$.toString(true))

  println(Route.SCHEMA$.toString(true))


  val schemaRegistryClient: SchemaRegistryClient =
    new CachedSchemaRegistryClient("http://172.16.129.58:8081", 1)
  schemaRegistryClient.getAllSubjects.forEach(println)

  //schemaRegistryClient.register("pradeep_movie-value", Movie.SCHEMA$)
  //schemaRegistryClient.register("pradeep_rating-value", Rating.SCHEMA$)
  //schemaRegistryClient.register("pradeep_enriched_movie-value", EnrichedMovie.SCHEMA$)


  /*schemaRegistryClient.register("summer2019_pradeep_trips-value", Trip.SCHEMA$)
  //schemaRegistryClient.register("summer2019_pradeep_calendar-value", Calendar.SCHEMA$)
  schemaRegistryClient.register("summer2019_pradeep_route-value", Route.SCHEMA$)
  schemaRegistryClient.register("summer2019_pradeep_enrichment-value", EnrichedTrip.SCHEMA$)

   */


  /*
  val pradeepSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata("pradeep_test")

  println(s"id: ${pradeepSchemaMetadata.getId}")
  println(s"version: ${pradeepSchemaMetadata.getVersion}")
  println(pradeepSchemaMetadata.getSchema)

  val pradeepSchema =
    schemaRegistryClient.getBySubjectAndID("pradeep_test",pradeepSchemaMetadata.getId)
  println(pradeepSchema.toString(true))

   */
}
