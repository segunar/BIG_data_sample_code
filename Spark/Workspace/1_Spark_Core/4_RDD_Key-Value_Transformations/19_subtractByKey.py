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
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [("A", 1), ("A", 2), ("B", 1), ("C", 1)] into an RDD.

    #          C1: parallelize
    # dataset1 -----------------> input1RDD

    input1RDD = sc.parallelize([("A", 1), ("A", 2), ("B", 1), ("C", 1)])

    # 2. Operation C2: Creation 'parallelize', so as to store the content of the collection [1,3,4] into an RDD.

    #          C1: parallelize
    # dataset1 -----------------> input1RDD
    #          C2: parallelize
    # dataset2 -----------------> input2RDD

    input2RDD = sc.parallelize([("A", 1.0), ("D", 1.0)])

    # 3. Operation T1: We apply subtractByKey, so as to remove from input1RDD any key appearing in input2RDD

    #          C1: parallelize
    # dataset1 -----------------> input1RDD ---| T1: subtractRDD
    #          C2: parallelize                 |---------------------> subtractedRDD
    # dataset2 -----------------> input2RDD ---|

    subtractedRDD = input1RDD.subtractByKey(input2RDD)

    # 4. Operation A1: collect the result

    #          C1: parallelize
    # dataset1 -----------------> input1RDD ---| T1: subtractRDD                      A1: collect
    #          C2: parallelize                 |---------------------> subtractedRDD ------------> resVAL
    # dataset2 -----------------> input2RDD ---|

    resVAL = subtractedRDD.collect()

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
