package seas.proc

import seas.model.{NodeInfo, Tree}
import scala.collection.mutable.StringBuilder
import org.apache.spark.mllib.tree.configuration.FeatureType.Continuous
import org.apache.spark.mllib.tree.model.Split
/**
 * Created by dpetzoldt on 6/26/15.
 */
class SASCode {

  type AnyProc = Proc[Nothing, Any]

  val code = new StringBuilder()

  def procHeader(p:AnyProc)={
    code ++= s"\n/********************************************************/\n/*** Process Step: ${p.toString}\n/********************************************************/\n\n"
    this
  }

  def sectionHeader(s:String)={
    code ++= s"\n/*********** $s ***********/\n\n"
    this
  }

  def ++=(s:String)={
    code++=s
    this
  }

  def declareNumber(varName:String) = {
    code ++= s"LENGTH $varName 8;\n"
    this
  }

  def toFile(path:String) {
    scala.tools.nsc.io.File(path).writeAll(code.toString)
  }

  def tree(tree: Tree, treeId: Int,varName:Array[String])={

    def catStr(s: Split) = s.categories.mkString("(", ",", ")")

    def condition(s: Split): String = {
      if (s.featureType == Continuous)
        s"${varName(s.feature)} <= ${s.threshold}"
      else
        s"${varName(s.feature)} in ${catStr(s)}"
    }

    def nodeStr(ni: NodeInfo) = ni.path.
      foldLeft(new StringBuilder())(
        (str, ni) => str
          ++= ni.node.id.toString
          ++= (ni.isLeft.map(if (_) " \\ " else " / ") getOrElse "")
      )

    val varPost = s"TREE_P_${treeId}"
    val varClas = s"TREE_C_${treeId}"

    var oldDepth = 0

    def posterior(ni:NodeInfo)={
      if (ni.node.predict.predict == 1) ni.node.predict.prob
      else  1 - ni.node.predict.prob
    }

    /* For each node ... */

    for (ni <- tree) {
      //code ++= ni.node.id.toString ++= " "

      if (ni.depth < oldDepth) {
        code ++= ("END;\n" * (oldDepth - ni.depth))
      }

      code ++= s"\n/***** Node: ${nodeStr(ni)} *****/\n" //(Tiefe:${ni.depth})

      if (ni.isRight getOrElse false) {
        code ++= s"END; ELSE DO;\n"
      }

      if (ni.node.isLeaf) {
        code ++= s"$varClas = ${ni.node.predict.predict};\n"
        code ++= s"$varPost = ${posterior(ni)};\n"
      } else {
        code ++= s"IF ${condition(ni.node.split.get)} THEN DO;\n"
      }

      oldDepth = ni.depth
    }
    code ++= ("END;\n" * oldDepth)
    this
  }

}
