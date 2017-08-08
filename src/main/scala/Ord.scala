/**
  * Scala equivalent of Java Comparable interface
  */
trait Ord {
    // Type Any is a super-type of all other types in Scala
    def < (that: Any): Boolean
    def <= (that: Any): Boolean = (this < that) || (this == that)
    def > (that: Any): Boolean = !(this <= that)
    def >= (that: Any): Boolean = !(this < that)
}