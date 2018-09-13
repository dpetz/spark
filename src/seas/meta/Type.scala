package seas.meta

/**
 * Created by dpetzoldt on 6/1/15.
 */
object Type {

  case object Num   extends Type("N")
  case object Chars extends Type("C")
  case object Unknown extends Type("UNKNOWN")

  def apply(id:String):Type=
    id match {
      case "N" => Num
      case "C" => Chars
    }
}

abstract sealed class Type(id:String) extends Attr(id)