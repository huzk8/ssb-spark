package main.scala

/**
 * SSB Query 3.1
 */
class Q31 extends SSBQuery {

    import spark.implicits._

    override def execute(): Unit = {

        var sql = """select c_nation, s_nation, d_year,
	sum(lo_revenue) as revenue
	from customer
	join lineorder
	  on lo_custkey = c_customerkey
	join supplier
	  on lo_suppkey = s_suppkey
	join dim_date
	  on lo_orderdatekey = d_datekey
	where
	c_region = 'ASIA'
	and s_region = 'ASIA'
	and d_year >= 1992 and d_year <= 1997
	group by c_nation, s_nation, d_year
	order by d_year asc, revenue desc"""

        val res = spark.sql(sql)
        outputDF(res)
    }

}
