package seas.proc

import seas.Data

//import java.io._

/**
 * Executable process step in an analytical pipeline.
 * Linked to an input process so can also be seen as the complete process until this step.
 * Immutable and caching its results once lazy execution has been triggered.
 * Typed in its input and output. Most processes use and/or produce [[Data]] and are controled by the meta data
 * Training processes can create processes for scoring in both Spark and SAS. (see [[score]] and [[toSAS]])
 * Ergebnis ist vom Typ R
 *
 * @see Inspired by [org.apache.spark.ml.Transformer]
 *      but with distinct capabilities on meta data use and score code genration
 */

abstract class Proc[-I,+R](processInput:Option[Proc[_,I]]) extends Serializable {

  type AnyProc = Proc[Nothing, Any]

  def input: Option[AnyProc] = processInput

  /** Process name. Same for all instances. Class name by default. */
  def name = this.getClass.getSimpleName

  /** Convenience constructor that wraps argument as an option. */
  def this(p: Proc[_, I]) = this(Option(p))

  /* Subprocesses if any. */
  def sub: List[AnyProc] = List()

  /** Helper method to create [[sub]] output if sub processes are linear chain.
    * @param last Adds recursively starining at this process step stopping at [[input]]
    */
  protected def linearSub(last: AnyProc): List[AnyProc] = {
    def addUntil(p: AnyProc): List[AnyProc] = {
      if (input.map(_ == p) getOrElse true) List(p)
      else p :: p.sub ::: addUntil(p.input.get)
    }
    addUntil(last)
  }

  /** Returns the data object at the source of this process flow*/
  def source=flow.last.asInstanceOf[Proc[_,Data]] // always second element in the flow


  /** List of this process and all his predecessors in increasing distance
    * (i.e. this process first; direct predecessor second, etc.)
    * Sub processes included.
    */
  //todo: optional  exclude subprocesses
  def flow: List[AnyProc] = input map (this :: sub ::: _.flow) getOrElse List(this)

  /** ******************** Ausführung und Ergebnisse *********************/

  /** Run process. Usually not called directly but called by [[result]] */
  protected def run: R

  /** Caches results of [[run]]. Initialized lazy, ie. triggers [[run]] at first access.
    * May trigger recursive execution of previos process steps. */
  lazy val result: R = run

  /** ******************** Konstruktion Nachfolger und Scorecode **********************/

  /** Creates scoring process for complete [[flow]].
    * @param in data object new process is append to instead of the training data
    * @throws UnsupportedOperationException if not implemented by sublasss
    */
  def score(in: Proc[Nothing, Data]): Proc[_, R] =
    throw new UnsupportedOperationException(
      s"Scoring not supported: $getClass")




  /* Erzeuge Nachfolger.*/
  def ->[T](next: Proc[I, R] => T): T = next(this)

  /** Copy process step with copy connected to new input.
    * @param in None if this step requires no input */
  def copy(in: Option[Proc[_, I]]): Proc[I, R] = copy(in.get)

  protected def copy(in: Proc[Nothing, I]): Proc[I, R]

  def toSAS: SASCode =
    throw new UnsupportedOperationException(
      s"No SAS support: $getClass")

  /** ******************** Load and Save to Disk **********************/

  // todo: Save Process

  // todo: Logging

  /*
  @SerialVersionUID(123L) // Class annotation
  def save(fileName: String) {
    val oos = new ObjectOutputStream(new FileOutputStream(fileName))
    oos.writeObject(this)
    oos.close
  }
  */
}


// todo: Load Process
/*
object Proc {

  def load(fileName:String){
    val ois = new ObjectInputStream(new FileInputStream(fileName))
    val obj = ois.readObject
    ois.close
    obj.asInstanceOf[Proc[Nothing, Any]]
  }

}
*/
