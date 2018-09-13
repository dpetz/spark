package seas.meta

/**
 * Created by dpetzoldt on 6/1/15.
 */
object Role {
  case object Target         extends Role("TARGET")
  case object Input          extends Role("INPUT")
  case object Rejected       extends Role("REJECTED")
  case object Id             extends Role("ID")
  case object Prediction     extends Role("PREDICTION")
  case object Classification extends Role("CLASSIFICATION")
  case object Segment        extends Role("SEGMENT")

  // Todo: Weitere

  def apply(id:String):Role=
    id match {
      case "TARGET"         => Target
      case "INPUT"          => Input
      case "REJECTED"       => Rejected
      case "ID"             => Id
      case "PREDICTION"     => Prediction
      case "CLASSIFICATION" => Classification
      case "SEGMENT"       => Segment
    }
}

abstract sealed class Role(id:String) extends Attr(id) {
  override def isRole=true
}