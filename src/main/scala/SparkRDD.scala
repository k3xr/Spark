import spark.implicits._

val pagecounts = sc.textFile("pagecounts-20100806-030000")

pagecounts.take(10).foreach(println(_))

pagecounts.count

val enPages = pagecounts.filter((s: String) => {
    val project = s.split(" ")(0)
    project.equals("en")
})

val enPagesTuple = enPages.map[(String, String, Long, Long)]((s: String) => {
    val fields = s.split(" ")
    (fields(0), fields(1), fields(2).toLong, fields(3).toLong)
})

enPagesTuple.take(10).foreach(println(_))

enPagesTuple.sortBy(_._4, ascending = false).take(5).foreach(println(_))

enPagesTuple
    .sortBy(_._3, ascending = false)
    .take(1)
    .foreach((t) => println("Name: " + t._2 + "\tVisits: " + t._3))

def histogram(inputRDD: org.apache.spark.rdd.RDD[(String, String, Long, Long)], numBins: Int): Array[(Long, Int)] = {

val minmax = inputRDD
    .map((t) => (t._3,t._3))
    .reduce((t1, t2) => (math.min(t1._1, t2._1), math.max(t1._2, t2._2)))

val binWidth = (minmax._2 - minmax._1)/numBins

val hist = enPagesTuple
    .map((t) => {
        val numVisits = t._3
        var bin = (numVisits - minmax._1) / binWidth
        if(bin >= numBins) { bin = numBins-1 }
        (bin, 1)
    })
    .reduceByKey(_+_)
    
    hist.collect

}