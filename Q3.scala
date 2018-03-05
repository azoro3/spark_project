import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import scala.util.Try
val data =sc.textFile("Z:/Doc/Cours/progDev/Perrin/datamin.csv")
val cleanData = data.map(line => {
    val splitedLine = line.split(",")
    val splittedArrayLength = splitedLine.length
    (splitedLine(splittedArrayLength-4),splitedLine(splittedArrayLength-3))
    })
val prePreData=cleanData.filter(line =>(!line._1.isEmpty && !line._2.isEmpty 
    && Try(line._1.toDouble).isSuccess 
    && Try(line._2.toDouble).isSuccess))
val preData = prePreData.map(s => Vectors.dense(s._1.toDouble, s._2.toDouble)).cache()
val numClusters = 10
val numIterations = 50
val clusters = KMeans.train(preData, numClusters, numIterations)
val WSSSE = clusters.computeCost(preData)
val centers = clusters.clusterCenters
val assignedData = clusters.predict(preData)
val clusterCountDesc = assignedData.map(t => (t, 1)).reduceByKey(_ + _).sortBy(_._2,false)
val clusterCountAsc = assignedData.map(t => (t, 1)).reduceByKey(_ + _).sortBy(_._2,true)
val troisP = clusterCountDesc.take(3)
val troisM=clusterCountAsc.take(3)
val finalAffectation = preData.cartesian(l_cluster_assignment)
troisM.saveAsTextFile("./out_question3_M")
troisP.saveAsTextFile("./out_question3_P")