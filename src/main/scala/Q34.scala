package main.scala

/**
 * SSB Query 3.4
 */
class Q34 extends SSBQuery {

    import spark.implicits._

    override def execute(): Unit = {

        var sql = """select c_city, s_city, d_year, sum(lo_revenue)
	as revenue
	from customer
	join lineorder
	  on lo_custkey = c_customerkey
	join supplier
	  on lo_suppkey = s_suppkey
	join date
	  on lo_orderdatekey = d_datekey
	where
	(c_city='UNITED KI1' or c_city='UNITED KI5')
	and (s_city='UNITED KI1' or s_city='UNITED KI5')
	and d_yearmonth = 'Dec1997'
	group by c_city, s_city, d_year
	order by d_year asc, revenue desc"""

        val res = spark.sql(sql)
        outputDF(res)
    }

}
