package seas.meta

/**
 *
 * @param col Column rank. First column has col==1
 */
case class Column(col:Int) extends Attr(col) {

  require(col>=1,s"Column not positive: $col")

  /* Returns True */
  override def isColumn=true

  /** col-1 for zero-based data structures */
  def idx=col-1

}
