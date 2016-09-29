package main.scala

/**
 * SSB Query 3.2
 */
class Q32 extends SSBQuery {

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
	c_nation = 'UNITED STATES'
	and s_nation = 'UNITED STATES'
	and d_year >= 1992 and d_year <= 1997
	group by c_city, s_city, d_year
	order by d_year asc, revenue desc"""

        val res = spark.sql(sql)
        outputDF(res)
    }

}
