package seas.proc

import seas.Data

/**
 * Created by dpetzoldt on 6/10/15.
 */
case class Filter(expr:String)(in:Proc[_,Data]) extends Proc[Data,Data](in)  {

  def run=in.result.copy(in.result.df.filter(expr))

  def copy(in:Proc[Nothing,Data])=Filter(expr)(in)

  override def score(d:Proc[Nothing,Data])=in.score(d)

  override def toSAS=in.toSAS

}
