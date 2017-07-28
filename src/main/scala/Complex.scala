/**
  * Scala Complex class example
  * @param real real component
  * @param imaginary imaginary component
  */
class Complex(real: Double, imaginary: Double) {
    def re(): Double = real
    def im(): Double = imaginary
    override def toString: String =
        "" + re + (if (im < 0) "" else "+") + im + "i"
}