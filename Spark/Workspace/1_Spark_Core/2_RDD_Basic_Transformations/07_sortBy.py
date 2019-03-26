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
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection ["This is my first line", "Hello", "Another line here"]
    # into an RDD. As we see, in this case our RDD is a collection of String items.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize(["This is my first line", "Hello", "Another line here"])

    # 2. Operation T1: Transformation 'sortBy', so as to get inputRDD sorted by the desired order we want.
    # The transformation operation 'sortBy' is a higher order function.
    # It requires as input arguments: (i) A function F and (ii) a collection of items C.
    # The function F specifies the weight we assign to each item.
    # In our case, given an RDD of String items, our function F will weight each item with the number of words that there are in the String.

    #         C1: parallelize             T1: sortBy
    # dataset -----------------> inputRDD ------------> sortedRDD

    sortedRDD = inputRDD.sortBy(lambda line: len(line.split(" ")))

    # 3. Operation A1: collect the items from sortedRDD

    #         C1: parallelize             T1: sortBy              A1: collect
    # dataset -----------------> inputRDD ------------> sortedRDD -------------> resVAL

    resVAL = sortedRDD.collect()

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
