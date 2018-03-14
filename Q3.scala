import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import scala.util.Try
val debut3 = System.currentTimeMillis();
val data =sc.textFile("Z:/Doc/Cours/progDev/Perrin/data.csv")
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
clusterCountDesc.saveAsTextFile("Z:/Doc/Cours/progDev/Perrin/out_question3_M")
clusterCountAsc.saveAsTextFile("Z:/Doc/Cours/progDev/Perrin/out_question3_P")
print("Q3: ")
print(System.currentTimeMillis - debut3)
print(" ms")
scala> print("Q3: ")
Q3:151854 ms