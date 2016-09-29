# ssb-spark


```
cd ssb-gen
make
./dbgen -s 10 -T s
./dbgen -s 10 -T d
./dbgen -s 10 -T p
./dbgen -s 10 -T c
./dbgen -s 10 -T l
cd ..
sbt package
export MASTER='local[8]'
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 11
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 12
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 13
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 21
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 22
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 23
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 31
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 32
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 33
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 34
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 41
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 42
time spark-submit --class "main.scala.SSBQuery" --master $MASTER target/scala-2.11/spark-ssb-queries_2.11-1.0.jar 43
```
