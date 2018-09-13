package seas.meta

/**
 * Created by dpetzoldt on 6/1/15.
 */
abstract sealed class Level(id:String) extends Attr(id) {
  override def isLevel=true
}

object Level {
  case object Interval extends Level("INTERVAL")
  case object Nominal  extends Level("NOMINAL")
  case object Ordinal  extends Level("ORDINAL")
  case object Binary   extends Level("BINARY")
  case object Unary    extends Level("UNARY")


  def apply(id:String):Level=
    id match {
      case "INTERVAL" => Interval
      case "NOMINAL"  => Nominal
      case "ORDINAL"  => Ordinal
      case "BINARY"   => Binary
      case "UNARY"    => Unary
    }
}