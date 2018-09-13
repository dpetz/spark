package seas.model

import org.apache.spark.rdd.RDD

/**
 * Created by dpetzoldt on 5/22/15.
 */

/**
 * Model Error Statistics with ability to calculate itself during RDD map and reduce.
 *
 * @param obs Number of observations error statistics are calculated from
 * @param mse Mean Squared Error
 * @param mae Mean Average Error
 */
 class Error(val obs:Int, val mse:Double, val mae:Double) extends Serializable{

  /** Returns weighted avergage of a statistic
    * @param stat Statistic to average as a function of Error
    * @param other Other error average with
    *
    * */
  protected def avg(stat:Error=>Double, other:Error):Double=
    (stat(this)*obs + stat(other)*other.obs) / (obs + other.obs)

  /* Reduces this error and other error as weighted averages */
  def +(e:Error)={
    Error(obs+e.obs, avg(_.mse,e), avg(_.mae,e))
  }

  override def toString= f"$obs Obs. | MSE=$mse%1.4f | MAE=$mae%1.4f "
}

object Error {

  /**  Creates [[Error]] for single observations.
    * @param target actual target value 
    * @param prediction Prediction from model
    *
    */
  def apply(target:Double , prediction:Double)={
    val dif = target - prediction
    new Error (1,dif*dif,dif.abs)
  }

  //def apply(p:Prediction):Error=apply(p.target, p.prediction)

  /** Creates [[BinaryError]].
    *
    * @param trg actual target value
    * @param cla predicted model classification
    * @param ev event value
    * @param nev non-event value
    */
  def apply(trg:Double , cla:Double, ev:Double=1, nev:Double=0)={
    require(trg == ev || trg == nev)
    require(cla == ev || cla == nev)
    val dif = trg - cla
    new BinaryError(1,dif*dif,dif.abs,
      if (cla==ev && trg==ev) 1 else 0,
      if (cla==nev && trg==nev) 1 else 0,
      if (cla==ev && trg==nev) 1 else 0,
      if (cla==nev && trg==ev) 1 else 0)
  }

  //def apply(p:Prediction,ev:Double=1, nev:Double=0):BinaryError=apply(p.target, p.prediction,ev,nev)

  def collect(rdd:RDD[Prediction],binaryTarget:Boolean)={
      if (binaryTarget)
        rdd.map((p:Prediction) => Error(p.target, p.prediction,1,0)).reduce(_ + _)
      else
        rdd.map((p:Prediction) => Error(p.target, p.prediction)).reduce(_ + _)
    }
}

/**
 * Error for binary targets providing confusion matrix based statistics.
 * Assumes  target values 1 (events) and 0 (non-events).
 *
 * @param tp  True Positives
 * @param tn  True Negatives
 * @param fp  False Positives
 * @param fn  False Negatives
 */
class BinaryError( obs:Int,  mse:Double, mae:Double, val tp:Int,val tn:Int,val fp:Int, val fn:Int) extends Error(obs,mse,mae) {

  def +(e:BinaryError)={
    new BinaryError(obs+e.obs, avg(_.mse,e), avg(_.mae,e),tp+e.tp,tn+e.tn,fp+e.fp,fn+e.fn)
  }

  override def +(e:Error)= this.+(e.asInstanceOf[BinaryError])

  override def toString= f"$obs Obs. | MCR=$mcr%1.4f | TPR=$tpr%1.4f | TNR=$tnr%1.4f"

  /** Missclassification Rat  */
  def mcr=1.0 * (fn + fp) / obs

  /** True Positive Rate */
  def tpr= 1.0 * tp / (tp + fn)

  /** True Negative Rate */
  def tnr= 1.0 * tn / (tn + fp)

  /** Precision */
  def precision= 1.0 * tp / (tp + fp)

  /** Recall */
  def recall= tpr




}