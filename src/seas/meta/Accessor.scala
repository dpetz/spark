package seas.meta

import org.apache.spark.sql.Row

/**
 * Extracts or calculates a variable value from a RDD [[Row]].
 * @see [[Accessor.rowByIndex]], [[Accessor.mapAtAccess]]
 */
abstract class Accessor extends Attr {
  def apply(s:Row):Any

  /** Like [[apply]] but assuming the value is a Double.
    * @throws ClassCastException If value is no Double
    */
  def dbl(s:Row)= apply(s).asInstanceOf[java.lang.Double].toDouble // @todo: Unboxing vermeiden

  /** Like [[apply]] but assuming the value is a String.
    * @throws ClassCastException If value is no String
    */
  def str(s:Row)=apply(s).asInstanceOf[String]

  override def isAccessor=true
}

object Accessor {

   case class RowByIndex(idx:Int) extends Accessor{
    def apply(r:Row)=r.get(idx)
    override def dbl(r:Row)=r.getDouble(idx)
    override def str(r:Row)=r.getString(idx)
  }

   case class MapAtAccess[R](v:Var,map:Map[Any,Any]) extends Accessor {
    def apply(r:Row)=map(v(r))
  }

  def rowByIndex(idx:Int)=new RowByIndex(idx) {
    override def dbl(s:Row)= {
      try {
        apply(s).asInstanceOf[java.lang.Double].toDouble // @todo: Unboxing vermeiden
      } catch {
        case cce:ClassCastException => throw new ClassCastException(s"Index: $idx, Value: ${apply(s)}" + s.mkString(","))
      }
    }

  }

  def mapAtAccess[R](v:Var,map:Map[Any,Any])=new MapAtAccess(v,map)

  def constant(value:Any)=new Accessor {def apply(s:Row)=value }

}

