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
# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, 2), (3, 8), (1, 4), (3, 4), (3, 6)] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([(1, 2), (3, 8), (1, 4), (3, 4), (3, 6)])

    # 2. Operation T1: Transformation 'reduceByKey', so as to get a new RDD ('reducedRDD') from inputRDD.

    # reduceByKey is a higher-order function.
    # It requires as input arguments: (i) A function F and (ii) a collection of items C which must be of type (key, value).
    # It produces as a result a new collection C' of type (key, value).

    # The amount of items in C' is equal to the amount of different keys available in C.
    # For each item ('k', 'v') in C', the value 'v' is computed from the aggregation of ['V1', 'V2', ..., 'Vk'],
    # where ['V1', 'V2', ..., 'Vk'] are the values of all items ('k', 'Vi') in C with key equal to 'k'.

    # The function F is the one responsible of the aggregation.
    # It takes two values 'v1' and 'v2' and aggregates them producing a new value v'.
    # As we see, the function F must preserve the data type of 'v1' and 'v2', as the result v' has to be of the same data type.

    # To produce a single value 'v' in C' from the original values ['V1', 'V2', ..., 'Vk'] in C, the function F has to be applied k-1 times.
    # To improve the efficiency, each node containing a subset of ['V1', 'V2', ..., 'Vk'] runs the aggregation for its subset.
    # For example, if ['V1', 'V2', ..., 'Vk'] are distributed among 3 nodes {N1, N2, N3} each node reduces its local subset to a single value
    # {v'1, v'2, v'3}. Finally, the values {v'1, v'2, v'3} are transferred over the network to aggregate them into the final value v'.

    #         C1: parallelize             T1: reduceByKey
    # dataset -----------------> inputRDD -----------------> reducedRDD

    reducedRDD = inputRDD.reduceByKey(lambda x, y: x + y)

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: reduceByKey              A1: collect
    # dataset -----------------> inputRDD ----------------> reducedRDD ------------> resVAL

    resVAL = reducedRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)


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
