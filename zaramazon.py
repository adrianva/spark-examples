from __future__ import division
import sys
import os

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row

from datetime import datetime

import json


os.environ['PYSPARK_PYTHON'] = sys.executable

GEOGRAPHIC_ZONES = ["zone-1", "zone-2", "zone-3", "zone-4", "zone-5"]


def parse_line(line):
    json_element = json.loads(line)
    purchase_date = datetime.strptime(json_element["timestamp"], '%Y-%m-%dT%H:%M:%S.%fZ')
    geographic_zone = assign_geographic_zone(json_element["location"]["lon"])

    return Row(
        id=int(json_element["product_id"]),
        purchase_date=purchase_date,
        item_type=json_element["item_type"],
        way=json_element["way"],
        shop_id=int(json_element["shop_id"]),
        price=float(json_element["price"]),
        payment_type=json_element["payment_type"],
        lon=float(json_element["location"]["lon"]),
        lat=float(json_element["location"]["lat"]),
        geographic_zone=geographic_zone
    )


def assign_geographic_zone(lon):
    if lon >= -180.0 and lon <=-108.0:
        zone = "zone-1"
    elif lon > -108.0 and lon <= -36.0:
        zone = "zone-2"
    elif lon > -36.0 and lon <= 36.0:
        zone = "zone-3"
    elif lon > 36.0 and lon <= 108.0:
        zone = "zone-4"
    elif lon > 108.0 and lon <= 180.0:
        zone = "zone-5"
    else:
        zone = None

    return zone


def sum_purchases(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


def update_state_by_key(purchases, update_function):
    return purchases\
        .updateStateByKey(update_function)\
        .transform(lambda x: x.sortBy(lambda (x, v): -v))


def reduce_by_key(purchases):
    return purchases\
        .reduceByKey(lambda a, b: a + b)\
        .transform(lambda x: x.sortBy(lambda (x, v): -v))


def reduce_by_key_and_window(purchases, window_duration, slide_duration):
    return purchases\
        .reduceByKeyAndWindow(lambda a, b: a + b,
                              lambda a, b: a - b,
                              windowDuration=window_duration,
                              slideDuration=slide_duration
                              )\
        .transform(lambda x: x.sortBy(lambda (x, v): -v))


def get_top_products_filtered_by_zone(kvs, zone, window_duration, slide_duration):
    return kvs.map(lambda purchase: parse_line(purchase[1]))\
            .filter(lambda purchase: purchase.geographic_zone == zone)\
            .map(lambda purchase: ((purchase.geographic_zone, purchase.id), 1))\
            .reduceByKeyAndWindow(lambda a, b: a + b,
                                  lambda a, b: a - b,
                                  windowDuration=window_duration,
                                  slideDuration=slide_duration
                                  )\
            .transform(lambda x: x.sortBy(lambda (x, v): -v))


def get_avg(rdd):
    total = rdd.map(lambda purchase: purchase[1]).reduce(lambda a, b: a + b)
    avg = rdd.map(lambda purchase: (purchase[0], purchase[1] * 100 / float(total)))
    return avg


if __name__ == "__main__":
    sc = SparkContext(appName="ZaramazonStreaming")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")

    brokers = "localhost:9092"
    topic = "zaramazonPurchase"

    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    purchases = kvs.map(lambda purchase: parse_line(purchase[1]))\
        .map(lambda purchase: (purchase.id, 1)).cache()

    # We have to store the state of the products in order to get the best product of all time
    strong_product = update_state_by_key(purchases, sum_purchases)

    # Our windows is equal to 10s so we simply have to obtain the top products with reduceByKey
    hot_product = reduce_by_key(purchases)

    # Emit the item types
    purchases_type = kvs.map(lambda purchase: parse_line(purchase[1]))\
        .map(lambda purchase: (purchase.item_type, 1))\

    # Get the top item types of all times and the top item types of the last hour
    item_type_purchases = update_state_by_key(purchases_type, sum_purchases)
    item_type_purchases_last_hour = reduce_by_key_and_window(purchases_type,
                                                             window_duration=3600,
                                                             slide_duration=10
                                                             )

    item_type_purchases = item_type_purchases.transform(get_avg)
    item_type_purchases_last_hour = item_type_purchases_last_hour.transform(get_avg)

    # Top 3 products by geographic zone
    top_products_by_zone = {}
    for zone in GEOGRAPHIC_ZONES:
        top_products_by_zone[zone] = get_top_products_filtered_by_zone(kvs,
                                                                       zone,
                                                                       window_duration=300,
                                                                       slide_duration=10
                                                                       )

    strong_product.pprint(5)
    hot_product.pprint(5)
    item_type_purchases.pprint(5)
    item_type_purchases_last_hour.pprint(5)

    for products in top_products_by_zone.values():
        products.pprint(3)

    ssc.start()
    ssc.awaitTermination()
