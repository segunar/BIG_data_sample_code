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
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection ["This is my first line", "Another line here"] into an RDD.
    # As we see, in this case our RDD is a collection of String items.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize(["This is my first line", "Another line here"])

    # 2. Operation T1: Transformation 'map', so as to get a new RDD ('wordsRDD') from inputRDD.
    # The transformation operation 'flatMap' is a higher order function.
    # It requires as input arguments: (i) A function F and (ii) a collection of items C.
    # It produces as output argument a new collection C' by applying F to each element of C.
    # What makes flatMap different from map is that the collection C' is flattened.

    # So, in our case, RDD2 is not going to be a collection of 2 items (each of them of type [String]).
    # Instead, RDD2 is going to be a collection of 8 items (each of them of type String).
    # This is because, after performing the entire map process, the resulting collection is flattened.

    #         C1: parallelize             T1: flatMap
    # dataset -----------------> inputRDD ------------> wordsRDD

    wordsRDD = inputRDD.flatMap(lambda line: line.split(" "))

    # 3. Operation P1: We persist wordsRDD, as we are going to use it more than once.
    # Please remember each action using an RDD will indeed trigger its lazy recomputation.
    # This might sound counter intuitive, but it makes perfect sense in the context of big data.
    # The idea is that we want to optimise the use of our resources.
    # Thus, if an action A requires an RDD to be computed, it is computed, used for the action A, but right after it discarded,
    # as we want to leverage as much as possible the memory of our nodes in the cluster.

    # Thus, if two actions A1 and A2 require the RDDi, it will be indeed recomputed twice, in this case leading to a waste of time.
    # To get the best tradeoff between memory usage and performance, we use the operation persist() to keep in memory any RDD that is
    # going to be used in more than 1 action. Thus, it will be computed just once.

    # In any case, persist can story the RDD in memory or in disk (or in a combination of both).
    # If it stores the RDD in memory, it does it by storing it in the heap area of the JVM of the executor process of each node.

    #         C1: parallelize             T1: flatMap            P1: persist    ------------
    # dataset -----------------> inputRDD ------------> wordsRDD -------------> | wordsRDD |
    #                                                                           ------------

    wordsRDD.persist()

    # 4. Operation A1: We count how many items are in the collection wordsRDD, to ensure there are 8 and not 2.

    #         C1: parallelize             T1: flatMap            P1: persist    ------------
    # dataset -----------------> inputRDD ------------> wordsRDD -------------> | wordsRDD |
    #                                                                           ------------
    #                                                                           |
    #                                                                           | A1: count
    #                                                                           |-----------> res1VAL
    #

    res1VAL = wordsRDD.count()

    # 5. We print by the screen the result value res1VAL
    print(res1VAL)

    # 6. Operation A2: collect the items from wordsRDD

    #         C1: parallelize             T1: flatMap            P1: persist    ------------
    # dataset -----------------> inputRDD ------------> wordsRDD -------------> | wordsRDD |
    #                                                                           ------------
    #                                                                           |
    #                                                                           | A1: count
    #                                                                           |-----------> res1VAL
    #                                                                           |
    #                                                                           | A2: collect
    #                                                                           |-------------> res2VAL

    res2VAL = wordsRDD.collect()

    # 7. We print by the screen the collection computed in res2VAL
    for item in res2VAL:
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
