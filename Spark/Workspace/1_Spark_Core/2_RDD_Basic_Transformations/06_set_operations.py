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
def my_main(sc, option):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1, 2, 1, 3] into an RDD.

    #          C1: parallelize
    # dataset1 -----------------> input1RDD

    input1RDD = sc.parallelize([1, 2, 1, 3])

    # 2. Operation C2: Creation 'parallelize', so as to store the content of the collection [1,3,4] into an RDD.

    #          C1: parallelize
    # dataset1 -----------------> input1RDD
    #          C2: parallelize
    # dataset2 -----------------> input2RDD

    input2RDD = sc.parallelize([1, 3, 4])

    # 3. Operation T1: We apply a set transformation based on the option being passed

    #          C1: parallelize
    # dataset1 -----------------> input1RDD ---| T1: set operation
    #          C2: parallelize                 |---------------------> setResultRDD
    # dataset2 -----------------> input2RDD ---|

    setResultRDD = None

    # Please note that all operations except 2 require shuffling all the data over the network to ensure that the operation is performed correctly.
    # Shuffling the data is undesirable (both in terms of time and network congestion), so use it only when really needed.
    # Also note that the operation 2 does not require to shuffle the data just because Spark union allows duplicates
    # (which is a bit different from the semantics of classical union operation in set theory).
    if (option == 1):
        setResultRDD = input1RDD.distinct()
    if (option == 2):
        setResultRDD = input1RDD.union(input2RDD)
    if (option == 3):
        setResultRDD = input1RDD.intersection(input2RDD)
    if (option == 4):
        setResultRDD = input1RDD.subtract(input2RDD)
    if (option == 5):
        setResultRDD = input1RDD.cartesian(input2RDD)

    # 4. Operation A1: collect the result

    #          C1: parallelize
    # dataset1 -----------------> input1RDD ---| T1: set operation                  A1: collect
    #          C2: parallelize                 |---------------------> setResultRDD ------------> resVAL
    # dataset2 -----------------> input2RDD ---|

    resVAL = setResultRDD.collect()

    # 5. We print by the screen the collection computed in resVAL
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
    # 1. We use as many input arguments as needed
    option = 1


    # 2. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 3. We call to my_main
    my_main(sc, option)
