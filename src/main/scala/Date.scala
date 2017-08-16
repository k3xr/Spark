/**
  *
  * @param y year
  * @param m month
  * @param d day
  */
class Date(y: Int, m: Int, d: Int) extends Ord {

    def year: Int = y

    def month: Int = m

    def day: Int = d

    override def toString: String = year + "-" + month + "-" + day

    override def equals(that: Any): Boolean =
        that.isInstanceOf[Date] && {
            val o = that.asInstanceOf[Date]
            o.day == day && o.month == month && o.year == year
        }

    def <(that: Any): Boolean = {
        if (!that.isInstanceOf[Date])
            sys.error("cannot compare " + that + " and a Date")

        val o = that.asInstanceOf[Date]
        (year < o.year) ||
            (year == o.year && (month < o.month ||
                (month == o.month && day < o.day)))
    }
}