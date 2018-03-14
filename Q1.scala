val debut1 = System.currentTimeMillis();
val data =sc.textFile("Z:/Doc/Cours/progDev/Perrin/data.csv")
val mappedData =data.map(line => line.split(","))
val selectedData =mappedData.map(line=> (line(5),1))
val reducedData =selectedData.reduceByKey(_+_)
val sortedData =reducedData.sortBy(_._2)
sortedData.saveAsTextFile("Z:/Doc/Cours/progDev/Perrin/out_question1")
print("Q1: ")
print(System.currentTimeMillis - debut1)
print(" ms")
Q1:60270 ms