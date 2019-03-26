# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import pyspark

# ------------------------------------------
# FUNCTION my_map_values
# ------------------------------------------
def my_map_values(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, "hello world"), (2, "bye bye"), (1, "nice to see you")]
    # into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([(1, "hello world"), (2, "bye bye"), (1, "hello nice to see you")])

    # 2. Operation T1: Transformation 'mapValues', so as to get a the lenght of each value per (key, value) in inputRDD.

    # mapValues is a higher-order function.
    # It requires as input arguments: (i) A function F and (ii) a collection of items C which must be of type (key, value).
    # If applies F(value) to each item of the collection, thus producing as a result a new collection C' of type (key, value).

    # Please note that the function do not alter the keys of the collection items.
    # Thus, in terms of efficiency, the operation respects the existing partitions in the RDD.

    #         C1: parallelize             T1: reduceByKey
    # dataset -----------------> inputRDD -----------------> reducedRDD

    mappedRDD = inputRDD.mapValues(lambda x: len(x.split(" ")))

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: mapValues                A1: collect
    # dataset -----------------> inputRDD ----------------> mappedRDD ------------> resVAL

    resVAL = mappedRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)


# ------------------------------------------
# FUNCTION my_flat_map_values
# ------------------------------------------
def my_flat_map_values(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, "hello world"), (2, "bye bye"), (1, "nice to see you")]
    # into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([(1, "hello world"), (2, "bye bye"), (1, "hello nice to see you")])

    # 2. Operation T1: Transformation 'mapValues', so as to get a the lenght of each value per (key, value) in inputRDD.

    # flatMapValues is a higher-order function.
    # It requires as input arguments: (i) A function F and (ii) a collection of items C which must be of type (key, value).
    # If applies F(value) to each item of the collection.
    # The application of F over value is supposed to return an iterator I[value1, value2, ..., valuek].
    # Thus, flatMapValues produces a new entry (key, valuei) per value in I.

    # Please note that the function do not alter the keys of the collection items.
    # Thus, in terms of efficiency, the operation respects the existing partitions in the RDD.

    #         C1: parallelize             T1: reduceByKey
    # dataset -----------------> inputRDD -----------------> reducedRDD

    mappedRDD = inputRDD.flatMapValues(lambda x: x.split(" "))

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: mapValues                A1: collect
    # dataset -----------------> inputRDD ----------------> mappedRDD ------------> resVAL

    resVAL = mappedRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)

    # ------------------------------------------


# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    print("\n\n--- [BLOCK 1] mapValues ---")
    my_map_values(sc)

    print("\n\n--- [BLOCK 2] flatMapValues ---")
    my_flat_map_values(sc)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 2. We call to my_main
    my_main(sc)
