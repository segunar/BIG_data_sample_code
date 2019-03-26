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
    pairRDD = sc.parallelize([(1, 2), (3, 4), (3, 6)])
    pairRDD.persist()

    print("--- count_by_key ---")
    countRDD = pairRDD.countByKey().items()

    for w in countRDD:
        print(w)

    print("--- lookup_key ---")
    valuesForAKeyRDD = pairRDD.lookup(3)

    for w in valuesForAKeyRDD:
        print(w)


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

