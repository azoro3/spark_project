val dataq1 =sc.textFile("Z:/Doc/Cours/progDev/Perrin/datamin.csv")
val mappedData =data.map(line => line.split(","))
val selectedData =mappedData.map(line=> (line(5),1))
val reducedData =selectedData.reduceByKey(_+_)
val sortedData =reducedData.sortBy(_._2)
sortedData.saveAsTextFile("./out_question1")