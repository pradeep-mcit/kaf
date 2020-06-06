package ca.mcit.bigdata.kafka

case class pra(start_date: String,
               start_station_code: String,
               end_date: String,
               end_station_code: String,
               duration_sec: String,
               is_member: String,

                   )

object pra {

  def apply(csvLine: String): pra = {
    val p = csvLine.split(",", -1)
    new pra(p(0), p(1), p(2),p(3), p(4), p(5))
  }

  def toCsv(Pra: pra): String = {
    Pra.start_date + "," +
      Pra.start_station_code + "," +
      Pra.end_date + "," +
      Pra.end_station_code + "," +
      Pra.duration_sec + "," +
      Pra.is_member

  }
}
