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
# FUNCTION my_ordering
# ------------------------------------------
def my_ordering(x):
    # 1. We create the output variable
    res = x * 1

    # 2. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4])

    # 2. Operation P1: We persist inputRDD, as we are going to use it more than once.

    #         C1: parallelize             P1: persist    ------------
    # dataset -----------------> inputRDD -------------> | inputRDD |
    #                                                    ------------

    inputRDD.persist()

    # 3. Operation A1: Action 'top', so as to take only a subset of the top items of the RDD.

    #         C1: parallelize             P1: persist    ------------
    # dataset -----------------> inputRDD -------------> | inputRDD |
    #                                                    ------------
    #                                                    |
    #                                                    | A1: top
    #                                                    |---------> res1VAL     --- Iterator of items int ---

    res1VAL = inputRDD.top(2)

    # 4. We print by the screen the collection computed in res1VAL
    print("top(2):")
    for item in res1VAL:
        print(item)

    # 5. Operation A1: Action 'top', so as to take only a subset of the top items of the RDD.

    #         C1: parallelize             P1: persist    ------------
    # dataset -----------------> inputRDD -------------> | inputRDD |
    #                                                    ------------
    #                                                    |
    #                                                    | A1: top
    #                                                    |---------> res1VAL     --- Iterator of items int ---
    #                                                    |
    #                                                    | A2: takeOrdered
    #                                                    |-----------------> res2VAL     --- Iterator of items int ---

    res2VAL = inputRDD.takeOrdered(2, my_ordering)

    # 6. We print by the screen the collection computed in res2VAL
    print("takeOrdered(2)(my_ordering):")
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

