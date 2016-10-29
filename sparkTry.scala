val nums = Array((1 to 50):_*)
val oneRDD = sc.parallelize(nums)
oneRDD.count
val otherRDD = oneRDD.map(_ * 2)
val result = otherRDD.collect
for (v <- result) println(v)
val sum = oneRDD.reduce(_ + _)
print(sum)