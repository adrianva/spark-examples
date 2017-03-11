from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row

from datetime import datetime


def parse_line(line):
    line_fields = line.split("::")
    match_date = datetime.strptime(line_fields[7], '%d/%m/%Y')
    return Row(
        id=int(line_fields[0]),
        season=line_fields[1],
        match_number=int(line_fields[2]),
        local_team=line_fields[3],
        visiting_team=line_fields[4],
        local_goals=int(line_fields[5]),
        visiting_goals=int(line_fields[6]),
        match_date=match_date,
        match_timestamp=line_fields[8]
    )


def sum_points(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


def get_points(match):
    if match.local_goals > match.visiting_goals:
        return [(match.local_team, 3), (match.visiting_team, 0)]
    elif match.local_goals < match.visiting_goals:
        return [(match.visiting_team, 3), (match.local_team, 0)]
    else:
        return [(match.local_team, 1), (match.visiting_team, 1)]


if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")

    brokers = "localhost:9092"
    topic = "soccer_match"

    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda match: parse_line(match[1]))\
        .map(lambda match: get_points(match))\
        .flatMap(lambda match: match)\
        .reduceByKey(lambda a, b: a + b)\
        .updateStateByKey(sum_points) \
        .transform(lambda x: x.sortBy(lambda (x, v): -v))

    lines.pprint()
    ssc.start()
    ssc.awaitTermination()
