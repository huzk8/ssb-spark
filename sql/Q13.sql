select sum(lo_extendedprice*lo_discount) as
revenue
from lineorder join date on lo_orderdatekey = d_datekey
where 
d_weeknuminyear = 6
and d_year = 1994
and lo_discount between 5 and 7
and lo_quantity between 26 and 35;