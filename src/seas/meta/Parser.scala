package seas.meta

/**
 * Parses values of a [[Var]] from a text source (e.g. csv files).
 */
abstract class Parser extends Attr {
  def parse(s:String):Any
  override def isParser=true
}

/** Parser Factory */
object Parser {

  /** Uses source string as is. Only modification is trimming. */
  val string=new Parser{
    def parse(s:String)=s.trim
  }

  /** Converts source string to [[Double]]
    * @throws ClassCastException if not convertible
    */
  val double=new Parser{
    def parse(s:String)=
      if (s.isEmpty) Double.NaN else s.toDouble
  }

}

