/**
  *
  */
object IntegerReference {
    def main(args: Array[String]) {
        val cell = new Reference[Int]
        cell.set(13)
        println("Reference contains the half of " + (cell.get * 2))
    }
}