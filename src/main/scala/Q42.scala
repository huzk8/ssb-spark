package main.scala

/**
 * SSB Query 4.2
 */
class Q42 extends SSBQuery {

    import spark.implicits._

    override def execute(): Unit = {

        var sql = """select d_year, s_nation, p_category,
	sum(lo_revenue - lo_supplycost) as profit
	from lineorder
	join date 
	  on lo_orderdatekey = d_datekey
	join customer
	  on lo_custkey = c_customerkey
	join supplier
	  on lo_suppkey = s_suppkey
	join part
	  on lo_partkey = p_partkey
	where
	c_region = 'AMERICA'
	and s_region = 'AMERICA'
	and (d_year = 1997 or d_year = 1998)
	and (p_mfgr = 'MFGR#1'
	or p_mfgr = 'MFGR#2')
	group by d_year, s_nation, p_category
	order by d_year, s_nation, p_category"""

        val res = spark.sql(sql)
        outputDF(res)
    }

}
