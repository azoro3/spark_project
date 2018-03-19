val debut4 = System.currentTimeMillis();
val data =sc.textFile("Z:/Doc/Cours/progDev/Perrin/data.csv")
val splittedData = data.map(line => {
    val splitedLine = line.split(",")
    val splittedArrayLength = splitedLine.length
    (splitedLine(splittedArrayLength-15),splitedLine(splittedArrayLength-4),splitedLine(splittedArrayLength-3))
    })
val filteredData = splittedData.filter(t => (t._1.equals("true")))
val mappedData = filteredData.map(line => (line,1)).reduceByKey(_+_)
mappedData.saveAsTextFile("Z:/Doc/Cours/progDev/Perrin/out_question4")
Q4:55500 ms