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
# FUNCTION my_mult
# ------------------------------------------
def my_mult(x, y):
    # 1. We create the output variable
    res = x * y

    # 2. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4] into an RDD.
    # Please note that the name parallelize is a false friend here. Indeed, the entire RDD is to be stored in a single machine,
    # and must fit in memory.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4])

    # 2. Operation P1: We persist inputRDD, as we are going to use it more than once.

    #         C1: parallelize             P1: persist    ------------
    # dataset -----------------> inputRDD -------------> | inputRDD |
    #                                                    ------------

    # 3. Operation A1: Action 'reduce', so as to get one aggregated value from inputRDD.

    # The action operation 'reduce' is a higher order function.
    # It is the most common basic action. With reduce(), we can easily sum the elements of our RDD, count the number of elements,
    # and perform other types of aggregations.
    # It requires as input arguments: (i) A function F and (ii) an RDD.
    # It produces as output argument a single result, computed by aggregating the result of applying F over all items of RDD (taken two by two).
    # Example: reduce (+) [1,2,3,4] = 10 (computed by doing 1 + 2 = 3, 3 + 3 = 6 and 6 + 4 = 10)
    # where F is (+), C is [1,2,3,4] and C' is 10

    # In our case, the collection C is always going to be RDD1, and C' the new RDD2.
    # Thus, the only thing we are missing is specifying F.
    # As you see, F must be a function receiving just 2 parameter (the items i1 and i2 of C we want to apply F(i1, i2) to).

    # We can define F with a lambda abstraction.

    #         C1: parallelize             P1: persist    ------------
    # dataset -----------------> inputRDD -------------> | inputRDD |
    #                                                    ------------
    #                                                    |
    #                                                    | A1: reduce
    #                                                    |------------> res1VAL
    #

    res1VAL = inputRDD.reduce(lambda x, y: x + y)

    # 4. We print by the screen the result computed in res1VAL
    print(res1VAL)

    # 5. Operation A2: Action 'reduce', so as to get one aggregated value from inputRDD.

    # In this case we define F with our own function.

    #         C1: parallelize             P1: persist    ------------
    # dataset -----------------> inputRDD -------------> | inputRDD |
    #                                                    ------------
    #                                                    |
    #                                                    | A1: reduce
    #                                                    |------------> res1VAL
    #                                                    |
    #                                                    | A2: reduce
    #                                                    |------------> res2VAL

    res2VAL = inputRDD.reduce(my_mult)

    # 6. We print by the screen the result computed in res2VAL
    print(res2VAL)


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


