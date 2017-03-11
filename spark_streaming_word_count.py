from pyspark import SparkContext
from pyspark.streaming import StreamingContext

"""
IMPORTANT: WE SHOULD RUN THIS SCRIPT UNDER SPARK 1.6

In order to run it we have to open two consoles:
In the first one we type nc -lk 9999.
In the second one we type:
bin/spark-submit --jars lib/spark-streaming-kafka-assembly_2.10-1.6.2.jar
/Users/adrian/Documents/proyectos/spark_tests/spark_streaming_word_count.py

Now, we are prepared to type something in the first console,
and the second one should output the result of the wordcount.
"""

if __name__ == "__main__":
    sc = SparkContext(appName="StreamTest", master="local[*]")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("checkpoint")

    lines = ssc.socketTextStream("localhost", 9999)
    counts = lines.flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a+b)

    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
