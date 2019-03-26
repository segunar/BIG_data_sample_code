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
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, 2), (3, 8), (1, 4), (3, 4), (3, 6)] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([(1, 2), (3, 8), (1, 4), (3, 4), (3, 6)])

    # 2. Operation T1: Transformation 'groupByKey', so as to get a new RDD ('groupedRDD') from inputRDD.

    # groupByKey is a higher-order function.
    # It requires as input argument: (i) a collection of items C which must be of type (key, value).
    # It produces as a result a new collection C' of type (key, [value]).

    # The amount of items in C' is equal to the amount of different keys available in C.
    # For each item ('k', 'v') in C', the value 'v' is computed from the list ['V1', 'V2', ..., 'Vk'],
    # where ['V1', 'V2', ..., 'Vk'] are the values of all items ('k', 'Vi') in C with key equal to 'k'.

    #         C1: parallelize             T1: groupByKey
    # dataset -----------------> inputRDD ----------------> groupedRDD

    groupedRDD = inputRDD.groupByKey()

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: groupByKey              A1: collect
    # dataset -----------------> inputRDD ---------------> groupedRDD ------------> resVAL

    resVAL = groupedRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        my_line = "(" + str(item[0]) + ", ["

        if (len(item[1]) > 0):
            for val in item[1]:
                my_line = my_line + str(val) + ","
            my_line = my_line[:-1]

        my_line = my_line + "])"

        print(my_line)


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
