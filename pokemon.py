import os.path
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext


def fix_pokemon(pokemon):
    if pokemon.prev_evolution:
        prev_evolution = pokemon.prev_evolution
        is_evolutioned = True
    else:
        prev_evolution = []
        is_evolutioned = False

    if pokemon.next_evolution:
        next_evolution = pokemon.next_evolution
    else:
        next_evolution = []

    fixed_pokemon = Row(
        avg_spawns=pokemon.avg_spawns,
        candy=pokemon.candy,
        candy_count=pokemon.candy_count,
        egg=pokemon.egg,
        height=pokemon.height,
        fixed_height=float(pokemon.height.split()[0]),
        id=pokemon.id,
        img=pokemon.img,
        multipliers=pokemon.multipliers,
        name=pokemon.name,
        next_evolution=next_evolution,
        num=pokemon.num,
        prev_evolution=prev_evolution,
        spawn_chance=pokemon.spawn_chance,
        spawn_time=pokemon.spawn_time,
        type=pokemon.type,
        weaknesses=pokemon.weaknesses,
        weight=pokemon.weight,
        fixed_weight=float(pokemon.weight.split()[0]),
        is_evolutioned=is_evolutioned
    )
    return fixed_pokemon


if __name__ == "__main__":
    warehouse_location = '/Users/adrian/Documents/proyectos/spark-tests/spark-warehouse'

    spark = SparkSession \
        .builder \
        .appName("Python Spark Dataframes example") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

    spark_context = spark.sparkContext
    sql_context = SQLContext(spark_context)

    filename = os.path.join('data', 'pokemon.json')

    df = spark.read.json(filename)

    # TODO We could use the method df.withColumn(new_column, length(new_column))
    # TODO from pyspark.sql.functions
    pokemons_rdd = df.rdd.map(lambda pokemon: fix_pokemon(pokemon))

    # We transform the dataframe to fix and add some useful columns to perform our analysis
    # Then we register the dataframe as a table to make use of SQL
    fixed_pokemons = sql_context.createDataFrame(pokemons_rdd)
    fixed_pokemons.registerTempTable("pokemons")

    print df.rdd.collect()
    print pokemons_rdd.collect()
    print fixed_pokemons.collect()

    print df.rdd.map(lambda pokemon: pokemon.height).take(1)

    correlation = fixed_pokemons.corr("fixed_weight", "fixed_height")
    print correlation

    most_common_weaknesses = sql_context.sql("select weakness, count(*) as times "
                                            "from ("
                                            "select explode(weaknesses) as weakness "
                                            "from pokemons "
                                            ") weaknesses "
                                            "group by weakness "
                                            "order by times desc "
                                            "limit 3"
                                             )

    normal_pokemons_not_in_eggs = sql_context.sql("select * from "
                                                 "pokemons "
                                                 "where array_contains(type, 'Normal') and egg='Not in Eggs'")

    print "3 most common weaknesses: {}".format(most_common_weaknesses.collect())
    print "Normal pokemons not in eggs: {}".format(normal_pokemons_not_in_eggs.collect())

    hive_context = HiveContext(spark_context)
    #hive_context.setConf("hive.warehouse.dir", "/Users/adrian/Documents/hive-warehouse")

    normal_pokemons_not_in_eggs.write.mode("overwrite").saveAsTable("default.normal_pokemons")

    # Show the content of the Hive table we've just created
    pokemons_normal_table = hive_context.table("default.normal_pokemons")
    pokemons_normal_table.show()

    hive_context.read.table("default.normal_pokemons")
    print "Normal Pokemons from Hive table: {}".format(sql_context.sql("select * from normal_pokemons").collect())
