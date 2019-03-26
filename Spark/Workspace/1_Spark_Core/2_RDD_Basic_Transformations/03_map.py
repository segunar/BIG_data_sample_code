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
# FUNCTION map_lambda
# ------------------------------------------
def map_lambda(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4, 5])

    # 2. Operation T1: Transformation 'map', so as to get a new RDD ('squareRDD') from inputRDD.
    # An RDD is inmutable, so it cannot be changed. However, you can apply a Transformation operation to get a new RDD2
    # by applying some operation on the content of RDD1.

    # The transformation operation 'map' is a higher order function.
    # It requires as input arguments: (i) A function F and (ii) a collection of items C.
    # It produces as output argument a new collection C' by applying F to each element of C.
    # Example: map (+1) [1,2,3] = [2,3,4]
    # where F is (+1), C is [1,2,3] and C' is [2,3,4]

    # In our case, the collection C is always going to be RDD1, and C' the new RDD2.
    # Thus, the only thing we are missing is specifying F.
    # As you see, F must be a function receiving just 1 parameter (the item of C we want to apply F(C) to).

    # In Spark we can define F via a lambda expression.

    #         C1: parallelize             T1: map
    # dataset -----------------> inputRDD --------> squareRDD

    squareRDD = inputRDD.map(lambda x: x * x)

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: map             A1: collect
    # dataset -----------------> inputRDD --------> squareRDD ------------> resVAL

    resVAL = squareRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)


# ------------------------------------------
# FUNCTION my_square_function
# ------------------------------------------
def my_square_function(x):
    # 1. We create the output variable
    res = x * x

    # 2. We return res
    return res


# ------------------------------------------
# FUNCTION map_explicit_function
# ------------------------------------------
def map_explicit_function(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4, 5])

    # 2. Operation T1: Transformation 'map', so as to get a new RDD ('squareRDD') from inputRDD.

    # map requires as input arguments: (i) A function F and (ii) a collection of items C.
    # map produces as output argument a new collection C' by applying F to each element of C.

    # In our case, the collection C is always going to be RDD1, and C' the new RDD2.
    # Thus, the only thing we are missing is specifying F.
    # As you see, F must be a function receiving just 1 parameter (the item of C we want to apply F(C) to).

    # In Spark we can define F via an explicit function. In this case we use the function my_square_function defined above.
    # You might seem confused by the way the line of code below is actually written
    # RDD2 = RDD1.map(F)
    # but this is the way it works. Spark will apply F(c) for each c in RDD1

    #         C1: parallelize             T1: map
    # dataset -----------------> inputRDD --------> squareRDD

    squareRDD = inputRDD.map(my_square_function)

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: map             A1: collect
    # dataset -----------------> inputRDD --------> squareRDD ------------> resVAL

    resVAL = squareRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)


# ------------------------------------------
# FUNCTION my_power
# ------------------------------------------
def my_power(a, b):
    # 1. We create the output variable
    res = a ** b

    # 2. We return res
    return res


# ------------------------------------------
# FUNCTION map_explicit_function_has_more_than_one_parameter
# ------------------------------------------
def map_explicit_function_has_more_than_one_parameter(sc, n):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4, 5])

    # 2. Operation T1: Transformation 'map', so as to get a new RDD ('squareRDD') from inputRDD.

    # map requires as input arguments: (i) A function F and (ii) a collection of items C.
    # map produces as output argument a new collection C' by applying F to each element of C.

    # In our case, the collection C is always going to be RDD1, and C' the new RDD2.
    # Thus, the only thing we are missing is specifying F.

    # As we saw before, F must be a function receiving just 1 parameter (the item of C we want to apply F(C) to).
    # This is a big limitation! What happens if we indeed want to apply a function F receiving 2 or more parameters?
    # We can walk around it by doing the following:
    # i) Let's call F2 the function we actually want to apply, where F2 has more than one parameter, for example 2 parameters
    # F2(c, extra_argument) = result
    # Now, let's define our actual F, the one map will indeed apply via the following lambda expression
    # F --> lambda c: F2(c, extra_argument)
    # As we see, map will still apply the function F receiving 1 parameter (happy times as F must only contain just 1 parameter).
    # However, F will be indeed nothing but a bypass function: You call me with c, I redirect you to F2(c, extra_parameter).
    # This way we can indeed use map to transform an RDD by applying a function with more than 1 parameter.

    #         C1: parallelize             T1: map
    # dataset -----------------> inputRDD --------> squareRDD

    powerRDD = inputRDD.map(lambda x: my_power(x, n))

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: map             A1: collect
    # dataset -----------------> inputRDD --------> powerRDD ------------> resVAL

    resVAL = powerRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, n):
    print("\n\n--- [BLOCK 1] map with F defined via a lambda expression ---")
    map_lambda(sc)

    print("\n\n--- [BLOCK 2] map with F defined via a explicit function ---")
    map_explicit_function(sc)

    print("\n\n--- [BLOCK 3] map where F requires more than one parameter ---")
    map_explicit_function_has_more_than_one_parameter(sc, n)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    n = 5

    # 2. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 3. We call to my_main
    my_main(sc, n)
