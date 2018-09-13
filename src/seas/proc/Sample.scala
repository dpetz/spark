package seas.proc

import seas.Data

/**
 * Wraps [[org.apache.spark.sql.DataFrame.sample]] as a [[Proc]].
 * @param fraction Sample rate
 * @param withReplacement if drawn with replacement. Default value: false
 * @param seed Random seed. Default: 12345
 */
case class Sample (fraction: Double, withReplacement: Boolean = false, seed: Long=12345)
  (in:Proc[_,Data]) extends Proc[Data,Data](in)  {

  /* Adds sample transformation to RDD. */
  def run=in.result.copy(in.result.df.sample(withReplacement,fraction,seed))

  def copy(in:Proc[Nothing,Data])=Sample(fraction,withReplacement,seed)(in)

  /* Empty score code */
  override def score(d:Proc[Nothing,Data])=in.score(d)

}


