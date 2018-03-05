val data =sc.textFile("Z:/Doc/Cours/progDev/Perrin/datamin.csv")
val splittedData = data.map(line => {
    val splitedLine = line.split(",")
    val splittedArrayLength = splitedLine.length
    (splitedLine(splittedArrayLength-15),splitedLine(splittedArrayLength-4),splitedLine(splittedArrayLength-3))
    })
val filteredData = splittedData.filter(t => (t._1.equals("true")||t._1.equals("false")))
val mappedData = filteredData.map(line => (line,1)).reduceByKey(_+_)
mappedData.saveAsTextFile("./out_qustion4")