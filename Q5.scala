val debut5 = System.currentTimeMillis();
val data =sc.textFile("Z:/Doc/Cours/progDev/Perrin/data.csv")
val mappedData =data.map(line => line.split(","))
val selectedData =mappedData.map(line=> (line(2)))
val mappedData2=selectedData.map(line => line.split(" "))
val selectedData2=mappedData2.map(line=>(line(0)))
val mappedData3=selectedData2.map(line => line.split("/"))
val selectedData3=mappedData3.map(line=>(line(0),1))
val reducedData =selectedData3.reduceByKey(_+_)
val sortedData =reducedData.sortBy(_._2,false)
val res=sortedData.take(3)
sortedData.saveAsTextFile("Z:/Doc/Cours/progDev/Perrin/out_question5")
Q5:8627 ms