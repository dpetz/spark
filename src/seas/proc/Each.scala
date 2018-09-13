package seas.proc
import seas.Data

/**
 * Generates a subprocess by applying an process step to each element of a sequence
 * @param generator Function creating new process step based on element and previous step
 *
 */
case class Each[T,R](elems:Traversable[T],generator:(T,Proc[_,R])=>Proc[R,R])(in:Proc[_,R]) extends Proc[R,R](in) {

  /** Reference to last step in sub process.
    * Generates sub process lazy at initialization
    */
  private val last:Proc[R,R] =
    elems.tail.foldLeft(generator(elems.head,in)) {
      (in:Proc[R,R],elem:T) => generator(elem,in)
    }

  override def sub=linearSub(last)

  def run=last.result

  def copy(in:Proc[Nothing,R])=Each(elems,generator)(in)

  override def score(d:Proc[Nothing,Data])=last.score(d)

}


