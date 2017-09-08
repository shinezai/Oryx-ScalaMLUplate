package com.shinezai.oryx.utils

import java.io.{IOException, InputStream, OutputStream}
import java.lang.Double.isNaN
import java.util.function.Function
import java.util.{Collections, Objects}

import com.cloudera.oryx.api.TopicProducer
import com.cloudera.oryx.api.batch.ScalaBatchLayerUpdate
import com.cloudera.oryx.common.collection.Pair
import com.cloudera.oryx.common.lang.ExecUtils
import com.cloudera.oryx.common.pmml.PMMLUtils
import com.cloudera.oryx.common.random.RandomManager
import com.cloudera.oryx.common.settings.ConfigUtils
import com.cloudera.oryx.ml.param.{HyperParamValues, HyperParams}
import com.google.common.base.Preconditions
import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dmg.pmml.PMML
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  * Created by wuxuan on 2017/7/12 0012.
  */
abstract class ScalaMLUpdate[M:ClassTag] extends ScalaBatchLayerUpdate[Object, M, String] with Serializable{
  private val log = LoggerFactory.getLogger(classOf[ScalaMLUpdate[_]])
  val MODEL_FILE_NAME = "model.pmml"
  private var testFraction:Double = .0
  private var candidates:Int = 0
  private var hyperParamSearch:String = ""
  private var evalParallelism = 0
  //in config,it is null
  private var threshold:java.lang.Double = null
  private var maxMessageSize = 0

  def this(config: Config) {
    this()
    this.testFraction = config.getDouble("oryx.ml.eval.test-fraction")
    var candidates = config.getInt("oryx.ml.eval.candidates")
    this.evalParallelism = config.getInt("oryx.ml.eval.parallelism")
    //下面定义为scala的Double的话，null值无法转化
    this.threshold = ConfigUtils.getOptionalDouble(config, "oryx.ml.eval.threshold")
    this.maxMessageSize = config.getInt("oryx.update-topic.message.max-size")
    Preconditions.checkArgument(testFraction >= 0.0 && testFraction <= 1.0)
    Preconditions.checkArgument(candidates > 0)
    Preconditions.checkArgument(evalParallelism > 0)
    Preconditions.checkArgument(maxMessageSize > 0)
    if (testFraction == 0.0) if (candidates > 1) {
      log.info("Eval is disabled (test fraction = 0) so candidates is overridden to 1")
      candidates = 1
    }
    this.candidates = candidates
    this.hyperParamSearch = config.getString("oryx.ml.eval.hyperparam-search")
  }

  protected def getTestFraction: Double = testFraction

  /**
    * @return a list of hyperparameter value ranges to try, one { @link HyperParamValues} per
    *  hyperparameter. Different combinations of the values derived from the list will be
    *  passed back into { @link #buildModel(JavaSparkContext,JavaRDD,List,Path)}
    */
  def getHyperParameterValues: java.util.List[HyperParamValues[_]] = Collections.emptyList()

  def buildModel(sparkContext: SparkContext, trainData: RDD[M], hyperParameters: java.util.List[_], candidatePath: Path): PMML

  def canPublishAdditionalModelData = false

  def publishAdditionalModelData(sparkContext: SparkContext, pmml: PMML, newData: RDD[M], pastData: RDD[M], modelParentPath: Path, modelUpdateTopic: TopicProducer[String, String]) {
    // Do nothing by default
  }

  def evaluate(sparkContext: SparkContext, model: PMML, modelParentPath: Path, testData: RDD[M], trainData: RDD[M]): Double

  override def configureUpdate(sparkContext: SparkContext,
                               timestamp: Long,
                               newKeyMessageData: RDD[(Object,M)],
                               pastKeyMessageData: RDD[(Object,M)],
                               modelDirString: String,
                               modelUpdateTopic: TopicProducer[String,String]): Unit = {

    Objects.requireNonNull(newKeyMessageData)
    val newData = newKeyMessageData.map(_._2)
    val pastData = if(pastKeyMessageData == null) null else pastKeyMessageData.map(_._2)

    if (null != newData) {
      newData.cache()
      newData.foreachPartition(p => {})
    }

    if (null != pastData) {
      pastData.cache()
      pastData.foreachPartition(p => {})
    }

    val hyperParameterCombos:java.util.List[java.util.List[_]] = HyperParams.chooseHyperParameterCombos(
      getHyperParameterValues, hyperParamSearch, candidates)

    val modelDir:Path = new Path(modelDirString)
    val tempModelPath:Path = new Path(modelDir, ".temporary")
    val candidatesPath:Path = new Path(tempModelPath, System.currentTimeMillis.toString)

    val  fs:FileSystem = FileSystem.get(modelDir.toUri, sparkContext.hadoopConfiguration)
    fs.mkdirs(candidatesPath)

    val bestCandidatePath:Path = findBestCandidatePath(sparkContext, newData, pastData, hyperParameterCombos, candidatesPath)

    val finalPath:Path = new Path(modelDir, System.currentTimeMillis.toString)
    if (bestCandidatePath == null) log.info("Unable to build any model")
    else {
      // Move best model into place
      fs.rename(bestCandidatePath, finalPath)
    }
    fs.delete(candidatesPath, true)

    if (modelUpdateTopic == null) {
      log.info("No update topic configured, not publishing models to a topic")
    } else {
      // Push PMML model onto update topic, if it exists
      val bestModelPath:Path = new Path(finalPath, MODEL_FILE_NAME)
      if (fs.exists(bestModelPath)) {
        val bestModelPathFS:FileStatus = fs.getFileStatus(bestModelPath)
        var bestModel:PMML = null
        val modelNeededForUpdates = canPublishAdditionalModelData
        val modelNotTooLarge = bestModelPathFS.getLen <= maxMessageSize
        var in: InputStream = null
        if (modelNeededForUpdates || modelNotTooLarge) {
          // Either the model is required for publishAdditionalModelData, or required because it's going to
          // be serialized to Kafka
          try {in = fs.open(bestModelPath)} catch {case e:IOException => log.info("fs.open error")}
          bestModel = PMMLUtils.read(in)
        }

        if (modelNotTooLarge) {
          modelUpdateTopic.send("MODEL", PMMLUtils.toString(bestModel))
        } else {
          modelUpdateTopic.send("MODEL-REF", fs.makeQualified(bestModelPath).toString)
        }

        if (modelNeededForUpdates) {
          publishAdditionalModelData(sparkContext, bestModel, newData, pastData, finalPath, modelUpdateTopic)
        }
      }
    }
    if (newData != null) newData.unpersist()
    if (pastData != null) pastData.unpersist()

  }

  private def findBestCandidatePath(sparkContext: SparkContext,
                                    newData: RDD[M],
                                    pastData: RDD[M],
                                    hyperParameterCombos: java.util.List[java.util.List[_]],
                                    candidatesPath: Path) = {
    import ScalaAndJavaFuncTrans.toJavaFunction
    val fck:Function[Integer, Pair[Path, java.lang.Double]] = (i:Integer) => buildAndEval(i, hyperParameterCombos, sparkContext, newData, pastData, candidatesPath)
    val getFirst:Function[Pair[Path, Double], Path] = (s:Pair[Path, Double]) => s.getFirst:Path
    val getSecond:Function[Pair[Path, Double], Double] = (s:Pair[Path, Double]) => s.getSecond:Double

    val pathToEval = ExecUtils.collectInParallel(
      candidates,
      Math.min(evalParallelism, candidates),
      true,
      fck,
      //Collectors.toMap(getFirst, getSecond, _, Supplier[util.HashMap]))
      //:Collector[Pair[Path, Double], _, util.Map[Path, Double]]
      //Collectors.toMap(getFirst, getSecond)
      PairGetFunc.toMap)

    var fs:FileSystem = null
    var bestCandidatePath:Path = null
    var bestEval = Double.NegativeInfinity
    import scala.collection.JavaConversions._
    for (pathEval <- pathToEval.entrySet) {
      val path = pathEval.getKey
      if (fs == null) fs = FileSystem.get(path.toUri, sparkContext.hadoopConfiguration)
      if (path != null && fs.exists(path)) {
        val eval = pathEval.getValue
        if (!isNaN(eval)) {
          // Valid evaluation; if it's the best so far, keep it
          if (eval > bestEval) {
            log.info(s"Best eval / model path is now $eval / $path")
            bestEval = eval
            bestCandidatePath = path
          }
        }
        else if (bestCandidatePath == null && testFraction == 0.0) {
          // Normal case when eval is disabled; no eval is possible, but keep the one model
          // that was built
          bestCandidatePath = path
        }
      } // else can't do anything; no model at all
    }
    //threshold此处的类型应为null
    if (threshold != null && bestEval < threshold) {
      log.info(s"Best model at $bestCandidatePath had eval $bestEval, but did not exceed threshold $threshold; discarding model")
      bestCandidatePath = null
    }
    bestCandidatePath
  }


  private def buildAndEval(i: Int,
                           hyperParameterCombos: java.util.List[java.util.List[_]],
                           sparkContext: SparkContext,
                           newData: RDD[M],
                           pastData: RDD[M],
                           candidatesPath: Path):Pair[Path, java.lang.Double] = {
    // % = cycle through combinations if needed
    val hyperParameters = hyperParameterCombos.get(i % hyperParameterCombos.size)
    val candidatePath = new Path(candidatesPath, Integer.toString(i))
    log.info("Building candidate {} with params {}", i, hyperParameters)
    val trainTestData = splitTrainTest(newData, pastData)
    val allTrainData = trainTestData.getFirst
    val testData = trainTestData.getSecond
    var eval = Double.NaN
    if (empty(allTrainData)) log.info("No train data to build a model")
    else {
      val model = buildModel(sparkContext, allTrainData, hyperParameters, candidatePath)
      if (model == null) log.info("Unable to build a model")
      else {
        val modelPath = new Path(candidatePath, MODEL_FILE_NAME)
        log.info("Writing model to {}", modelPath)
        try {
          val fs = FileSystem.get(candidatePath.toUri, sparkContext.hadoopConfiguration)
          fs.mkdirs(candidatePath)
          val out:OutputStream = fs.create(modelPath)
          try {
            PMMLUtils.write(model, out)
          }
          finally if (out != null) out.close()
        } catch {
          case ioe: IOException =>
            throw new IllegalStateException(ioe)
        }
        if (empty(testData)) log.info("No test data available to evaluate model")
        else {
          log.info("Evaluating model")
          eval = evaluate(sparkContext, model, candidatePath, testData, allTrainData)
        }
      }
    }
    log.info(s"Model eval for params $hyperParameters: $eval ($candidatePath)")
    new Pair[Path, java.lang.Double](candidatePath, eval)
  }

  private def splitTrainTest(newData: RDD[M], pastData: RDD[M]): Pair[RDD[M], RDD[M]] = {
    Objects.requireNonNull(newData)
    if (testFraction <= 0.0) return new Pair[RDD[M], RDD[M]](if (pastData == null) newData
    else newData.union(pastData), null)
    if (testFraction >= 1.0) return new Pair[RDD[M], RDD[M]](pastData, newData)
    if (empty(newData)) return new Pair[RDD[M], RDD[M]](pastData, null)
    val newTrainTest = splitNewDataToTrainTest(newData)
    val newTrainData = newTrainTest.getFirst
    new Pair[RDD[M], RDD[M]](if (pastData == null) newTrainData
    else newTrainData.union(pastData), newTrainTest.getSecond)
  }

  private def empty(rdd: RDD[_]) = rdd == null || rdd.isEmpty

  protected def splitNewDataToTrainTest(newData: RDD[M]): Pair[RDD[M], RDD[M]] = {
    val testTrainRDDs = newData.randomSplit(Array[Double](1.0 - testFraction, testFraction), RandomManager.getRandom.nextLong)
    new Pair[RDD[M], RDD[M]](testTrainRDDs(0), testTrainRDDs(1))
  }

}
