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
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(11, 2), (1, 5), (200, 3)] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([(16, "A"), (5, "A"), (213, "A")])

    # 2. Operation T1: Transformation 'sortByKey', so as to get a new RDD ('sortedRDD') from inputRDD.

    # sortByKey sorts an RDD with key/value pairs provided that there is an ordering defined on the key.
    # Once we have sorted our data, any subsequent call on the sorted data to collect() or save() will result in ordered data.
    # It provides as a parameter a function F assigning a weight to each (key, value) element, which is then used to do the sorting.

    #         C1: parallelize             T1: sortByKey
    # dataset -----------------> inputRDD -----------------> sortedRDD

    sortedRDD = inputRDD.sortByKey()

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: reduceByKey              A1: collect
    # dataset -----------------> inputRDD ----------------> reducedRDD ------------> resVAL

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
