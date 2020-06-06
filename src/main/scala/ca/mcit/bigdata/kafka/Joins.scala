/*package ca.mcit.bigdata.kafka

trait Joins[L, R, Q] {

  def join(a: List[L], b: List[R]): List[Q]

}

case class JoinOutput(left: Any, right: Option[Any])

// GenericMapJoin

class GenericMapJoin[L, R](val joinCond: (L) => String)(val joinCond1: (R) => String) extends Joins[L, R, JoinOutput] {
  override def join(a: List[L], b: List[R]): List[JoinOutput] = {


    val l: Map[String, R] = b
      .map(b => joinCond1(b) -> b)
      .toMap

    a
      .filter(a => l.contains(joinCond(a)))
      .map(a => JoinOutput(a, Some(l(joinCond(a)))))
  }
}

// Generic Nested loop join

class GenericNestedLoopJoin[L, R](val joinCond: (L, R) => Boolean) extends Joins[L, R, JoinOutput] {
  override def join(a: List[L], b: List[R]): List[JoinOutput] = for {
    i <- a
    j <- b
    if joinCond(i, j)
  } yield JoinOutput(i, Some(j))

}

 */