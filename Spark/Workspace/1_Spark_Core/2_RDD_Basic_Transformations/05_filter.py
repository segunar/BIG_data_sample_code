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
# FUNCTION filter_lambda
# ------------------------------------------
def filter_lambda(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,4,9,16] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 4, 9, 16])

    # 2. Operation T1: Transformation 'filter', so as to get a new RDD ('mult4RDD') from inputRDD.
    # An RDD is inmutable, so it cannot be changed. However, you can apply a Transformation operation to get a new RDD2
    # by applying some operation on the content of RDD1.

    # The transformation operation 'filter' is a higher order function.
    # It requires as input arguments: (i) A function F and (ii) a collection of items C.
    # It produces as output argument a new collection C' by applying F to each element of C.
    # If F(c) returns True, the element c is added to the new collection C'. Otherwise it is filtered.
    # Example: filter (>3) [2,4,1,6] = [4,6]
    # where F is (>3), C is [2,4,1,6] and C' is [4,6]

    # In our case, the collection C is always going to be RDD1, and C' the new RDD2.
    # Thus, the only thing we are missing is specifying F.
    # As you see, F must be a function receiving just 1 parameter (the item of C we want to apply F(C) to).

    # In Spark we can define F via a lambda expression.

    #         C1: parallelize             T1: filter
    # dataset -----------------> inputRDD ----------> mult4RDD

    mult4RDD = inputRDD.filter(lambda x: x % 4 == 0)

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: filter            A1: collect
    # dataset -----------------> inputRDD -----------> mult4RDD ------------> resVAL

    resVAL = mult4RDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)


# ------------------------------------------
# FUNCTION my_filter_function
# ------------------------------------------
def my_filter_function(x):
    # 1. We create the output variable
    res = False

    # 2. We apply the filtering function
    if (x % 4 == 0):
        res = True

    # 3. We return res
    return res


# ------------------------------------------
# FUNCTION filter_explicit_function
# ------------------------------------------
def filter_explicit_function(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,4,9,16] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 4, 9, 16])

    # 2. Operation T1: Transformation 'filter', so as to get a new RDD ('squareRDD') from inputRDD.
    # Now we define F via our explicit function

    #         C1: parallelize             T1: map
    # dataset -----------------> inputRDD --------> squareRDD

    mult4RDD = inputRDD.filter(my_filter_function)

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: filter
    # dataset -----------------> inputRDD ----------> mult4RDD

    resVAL = mult4RDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)


# ------------------------------------------
# FUNCTION is_length
# ------------------------------------------
def is_length(a, b):
    # 1. We create the output variable
    res = False

    # 2. We apply the filtering function
    if (len(a) == b):
        res = True

    # 3. We return res
    return res


# ------------------------------------------
# FUNCTION filter_explicit_function_has_more_than_one_parameter
# ------------------------------------------
def filter_explicit_function_has_more_than_one_parameter(sc, n):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection ["Hello", "Sun", "Bye", "Cloud"] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize(["Hello", "Sun", "Bye", "Cloud"])

    # 2. Operation T1: Transformation 'filter', so as to get a new RDD ('size_nRDD') from inputRDD.

    # We apply now a lambda expression as F to bypass each item of the collection to our actual filtering function F2 requiring more than
    # one argument

    #         C1: parallelize             T1: filter
    # dataset -----------------> inputRDD ----------> size_nRDD

    size_nRDD = inputRDD.filter(lambda x: is_length(x, n))

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: filter             A1: collect
    # dataset -----------------> inputRDD -----------> size_nRDD ------------> resVAL

    resVAL = size_nRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, n):
    print("\n\n--- [BLOCK 1] filter with F defined via a lambda expression ---")
    filter_lambda(sc)

    print("\n\n--- [BLOCK 2] filter with F defined via a explicit function ---")
    filter_explicit_function(sc)

    print("\n\n--- [BLOCK 3] filter where F requires more than one parameter ---")
    filter_explicit_function_has_more_than_one_parameter(sc, n)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    n = 3

    # 2. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 3. We call to my_main
    my_main(sc, n)
