import java.util.{Date, Locale}
import java.text.DateFormat._

/**
  * Scala interaction with Java
  */
object SpanishDate {
    def main(args: Array[String]) {
        val now = new Date
        val spanish = new Locale("es", "ES")
        val df = getDateInstance(LONG, spanish)
        println(df format now)
    }
}
