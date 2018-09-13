package seas.model

import org.apache.spark.mllib.tree.model.Node
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.configuration.Algo.Algo


/** Kontextinfos für einen Baumknoten. Wurzel hat Tiefe 0 */
case class NodeInfo(node:Node, parent:Option[NodeInfo], depth:Int)  {
  /* Ist dieser Knoten ein linkes Kind */
  def isLeft = parent.map(_.left.get == this)
  def isRight = parent.map(_.right.get == this)
  private def down(child:Option[Node]):Option[NodeInfo]=child.map(NodeInfo(_,Option(this),depth+1))
  /*  Neue NodeInfo für linkes Kind oder None */
  def left= down(node.leftNode)
  /*  Neue NodeInfo für rechtes Kind oder None */
  def right= down(node.rightNode)
  /* Pfad zur Wurzel als Liste */
  def path:List[NodeInfo]= parent.map(this :: _.path) getOrElse List(this)
  //def equals(ni:NodeInfo)=node.id == ni.node.id
}

class Tree(val top:Node) extends Traversable[NodeInfo] {

  /** Bewegung durch den Baum */
  def foreach[U](f:NodeInfo=>U):Unit={

    /** Liste von Baumknoten */
    type NodeInfos = List[Option[NodeInfo]]

    def walk[U](nodes: NodeInfos)(f: NodeInfo => U): Unit = {

      /** Entfernen führender Nones aus Liste */
      def dropNones(nodes: NodeInfos): NodeInfos =
        nodes match {
          case Nil => Nil
          case Some(n) :: tail => nodes
          case None :: tail => dropNones(tail)
        }

      /** Rekursives Absteigen in den Baum. */
      dropNones(nodes) match {
        case Nil => ()
        case n :: tail => {
          f(n.get)
          walk(n.get.left :: n.get.right :: tail)(f)
        }
      }
    }
    walk[U](List(Option(NodeInfo(top,None,0))))(f)
  }

}
