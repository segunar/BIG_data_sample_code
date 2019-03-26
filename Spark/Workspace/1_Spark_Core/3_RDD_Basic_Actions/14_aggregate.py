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
# FUNCTION average_map_and_reduce
# ------------------------------------------
def average_map_and_reduce(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4,5] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4, 5])

    # 2. Operation T1: Transformation 'map', so as to get a new RDD ('pairRDD') from inputRDD.

    #         C1: parallelize             T1: map
    # dataset -----------------> inputRDD --------> pairRDD

    pairRDD = inputRDD.map(lambda x: (x, 1))

    # 3. Operation A1: Action 'reduce', so as to get one aggregated value from pairRDD.

    #         C1: parallelize             T1: map           A1: reduce
    # dataset -----------------> inputRDD --------> pairRDD ------------> resVAL

    resVAL = pairRDD.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # 4. We print by the screen the result computed in resVAL

    print("TotalSum = " + str(resVAL[0]))
    print("TotalItems = " + str(resVAL[1]))
    print("AverageValue = " + str((resVAL[0] * 1.0) / (resVAL[1] * 1.0)))


# ------------------------------------------
# FUNCTION aggregate_lambda
# ------------------------------------------
def aggregate_lambda(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4,5] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4, 5])

    # 2. Operation A1: Action 'aggregate', to aggregate all items of the RDD returning a value of a different datatype that such RDD items.

    # The action operation 'aggregate' requires 3 input arguments:

    # (i). An initial zero value of the type we want to return.
    # It will be received as initial zero value on each node of the cluster. It serves as initial accumulator.

    # (ii). A function F1 to aggregate the RDD local elements of each node.
    # F1 must receive as input 2 parameters: The accumulator and the new local item to aggregate it with.

    # (iii). A function F2 to aggregate the different accumulators contained in the different nodes hosting the RDD.
    # F2 must receive as input 2 parameters: The final accumulator computed by Node1 and the final accumulator computed by Node2.
    #                                        This function states how to combine such these 2 accumulators.

    # In this case we define F1 and F2 via lambda abstractions.

    resVAL = inputRDD.aggregate((0, 0),
                                lambda acc, e: (acc[0] + e, acc[1] + 1),
                                lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
                                )

    #         C1: parallelize             A1: aggregate
    # dataset -----------------> inputRDD --------------> resVAL

    # 3. We print by the screen the collection computed in resVAL

    print("TotalSum = " + str(resVAL[0]))
    print("TotalItems = " + str(resVAL[1]))
    print("AverageValue = " + str((resVAL[0] * 1.0) / (resVAL[1] * 1.0)))


# ------------------------------------------
# FUNCTION combine_local_node
# ------------------------------------------
def combine_local_node(accum, item):
    # 1. We create the variable to return
    res = ()

    # 2. We modify the value of res
    val1 = accum[0] + item
    val2 = accum[1] + 1

    # 3. We assign res properly
    res = (val1, val2)

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION combine_different_nodes
# ------------------------------------------
def combine_different_nodes(accum1, accum2):
    # 1. We create the variable to return
    res = ()

    # 2. We modify the value of res
    val1 = accum1[0] + accum2[0]
    val2 = accum1[1] + accum2[1]

    # 3. We assign res properly
    res = (val1, val2)

    # 3. We return res
    return res


# ------------------------------------------
# FUNCTION aggregate_own_functions
# ------------------------------------------
def aggregate_own_functions(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4,5] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4, 5])

    # 2. Operation A1: Action 'aggregate', to aggregate all items of the RDD returning a value of a different datatype that such RDD items.

    # The action operation 'aggregate' requires 3 input arguments:

    # (i). An initial zero value of the type we want to return.
    # It will be received as initial zero value on each node of the cluster. It serves as initial accumulator.

    # (ii). A function F1 to aggregate the RDD local elements of each node.
    # F1 must receive as input 2 parameters: The accumulator and the new local item to aggregate it with.

    # (iii). A function F2 to aggregate the different accumulators contained in the different nodes hosting the RDD.
    # F2 must receive as input 2 parameters: The final accumulator computed by Node1 and the final accumulator computed by Node2.
    #                                        This function states how to combine such these 2 accumulators.

    # In this case we define F1 and F2 via our own functions.

    resVAL = inputRDD.aggregate((0, 0),
                                combine_local_node,
                                combine_different_nodes
                                )

    # 3. We print by the screen the collection computed in resVAL

    print("TotalSum = " + str(resVAL[0]))
    print("TotalItems = " + str(resVAL[1]))
    print("AverageValue = " + str((resVAL[0] * 1.0) / (resVAL[1] * 1.0)))


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    print("\n\n--- [BLOCK 1] Compute Average via map + reduce ---")
    average_map_and_reduce(sc)

    print("\n\n--- [BLOCK 2] Compute Average via aggregate with lambda ---")
    aggregate_lambda(sc)

    print("\n\n--- [BLOCK 3] Compute Average via aggregate with our own functions ---")
    aggregate_own_functions(sc)


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
