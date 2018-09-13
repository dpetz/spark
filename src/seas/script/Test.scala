package seas.script


import org.apache.spark.{SparkConf, SparkContext}
import seas.Spark
import seas.meta.{Var, Meta}
import seas.proc._

import seas.stat.Freq

/**
 * Created by dpetzoldt on 5/18/15.
 */

object Test {
  val metaApr15 = "/home/dpetzoldt/2015/SEAS/Apr15CRO_Meta.csv"
  val dataApr15 =  "hdfs://bipower:8020/user/dpetzoldt/SEAS/Apr15CRO.csv"


  def forestApr15: Unit = {

    //import seas.script.Test._
    import seas.proc._
    import seas.model.Forest

    //seas.Spark += sc += sqlContext

    // maxDepth muss <=30 sein
    val train100 = Source(metaApr15,dataApr15) -> Filter("split < 2") -> Points -> Forest(numTrees=100,maxDepth=10,maxBins=100)

    println(train100.error)

    val test100 = train100.source -> Filter("split = 2") -> Score(train100)

    println(test100.error)

    test100.toSAS.toFile("/home/dpetzoldt/2015/SEAS/Forest_Apr15_100.txt")

  }

  val meta0512 = "/home/dpetzoldt/2015/SEAS/meta0512.csv"
  //val data0512 =  "hdfs://bipower:8020/user/dpetzoldt//SEAS/data0512.csv"
  val data0512 = "/home/dpetzoldt/2015/SEAS/data0512.csv"


  def forest0512 {

    //import seas.script.Test._
    import seas.proc._
    import seas.model.Forest

/*
    val test = Source(meta0512,data0512) -> Sample(.1)-> Filter("split < 2")
    val cats = test.result.meta.inputs.categorials
    val encode = test -> Each(cats,Encode())
    encode.result.df.count
    val points = encode -> Points
    points.mapper.features.mkString("\n")
  */

    val train = Source(meta0512,data0512) -> Sample(.1)-> Filter("split < 2") -> Points -> Forest(numTrees=10)


    println(train.error) // Stößt Prozessdurchlauf an

    val test = train.source -> Filter("split = 2") -> Score(train)

    println(test.error) // Initiiert Scoring

    test.toSAS.toFile("/home/dpetzoldt/2015/SEAS/score0512.txt")

    //val train = Source(meta0512,data0512) -> Sample(.1) -> Filter("split < 2") -> Each(factors,Encode()) -> ToPoints -> Forest(numTrees=10)
  }

  def readLine {
    import seas.meta.Meta
    val meta = Meta("/home/dpetzoldt/2015/SEAS/meta0512.csv")
    val line = "0,1,36,8,224,1,0,0,0,1,0,0,0,0,0,0,0,0,0,6,0,0,0,0,0,0,0,0,4,0,0,0,0,0,0,0,4,0,0,0,1,1,0,0,0,0,_OTHER_"
    val row = meta.readLine(line)
  }

  def encodeVar {

    //import seas.script.Test._
    import seas.proc._

    val data = Source(meta0512,data0512)

    val factors = data.result.meta.inputs.nominals

    val v = factors.head
    val trans = data -> Encode(v)
    val _v = trans.result.meta.last

    trans.result.df.rdd.map(_v(_)).countByValue()
  }

  def read {

    import org.apache.spark.mllib.tree.configuration.FeatureType.Continuous
    import seas.meta.Meta
    import seas.model.Forest

    //Spark.context.addJar("/home/dpetzoldt/2015/SEAS/Scala/out/artifacts/SEAS_jar/SEAS.jar")

    val vars = Meta("/home/dpetzoldt/2015/SEAS/meta0512.csv")
    val data = vars.readCSV("hdfs://bipower:8020/user/dpetzoldt//SEAS/data0512.csv")

    //Freq(data.rows,meta("target_journey").get).toSeq

    //data.levels.toString
/*
    val split = vars("split").rowAccess[Double]


    val trainData = data { _.filter(split(_) < 2 ) }
    val testData = data { _.filter(split(_) == 2 ) }

    val forest = Forest(trainData,numTrees=10)

    val trainErr = forest.trainFit.error
    val testErr = forest.validate(testData).error

    println(s"Training: $trainErr \n Test: $testErr")

    val top = forest.model.trees(1).topNode

    //forest.model.save(Spark.context, "/home/dpetzoldt/2015/SEAS/forest0512.txt")
    //val sameModel = RandomForestModel.load(sc, "myModelPath")

    import seas.model.Tree

    val tree = new Tree(top)
    val cat = tree.find(n => !n.node.isLeaf && n.node.split.get.featureType != Continuous)

    tree.size

    //tree.filter(_.isLeaf).size
*/

/*
    scala.tools.nsc.io.File("filename").writeAll(top.)

    def condition(n:Node)=
      n.split.get.featureType match {
        case Continuous => s"<= ${s.threshold}"
        case Categorical => s"IN (${s.categories.mkString(",")})"
      }


    tree.foldLeft()

    case class ScorecodeAlt(code:String,prev:Node,curr:Node){



    }

    def scorecode(n:Node,code:StringBuilder,clas="clas",prob="prob"):String={
      if (n.isLeaf)
        s"${clas}=${n.predict}\n${prob}=${n.predict}"

        if  n.

        else
      s"IF ${data.features(n.split.get.feature).name} ${condition(n)} THEN DO;\n"

        }

      // IF <Condition> THEN DO;
      // <Prediction>
      // END;
      // ELSE DO;
      // <Prediction>
      // END;





      for (n <-tree) {
        n.prob
      }

    }
    scorecode(tree)
*/

//model.trees(0).topNode.leftNode
/*
numTrees=100:
Training: 69999 Beob. | MSE=0,3081 | MAE=0,3081 | Miss.=0,3081
 Test: 30001 Beob. | MSE=0,3016 | MAE=0,3016 | Miss.=0,3016
*/



  }
}