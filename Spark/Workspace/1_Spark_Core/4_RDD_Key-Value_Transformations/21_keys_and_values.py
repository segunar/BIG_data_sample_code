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
# FUNCTION my_keys
# ------------------------------------------
def my_keys(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, "A"), (1, "B"), (2, "B")] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([(1, "A"), (1, "B"), (2, "B")])

    # 2. Operation T1: Transformation 'keys', so as to get a new RDD with the keys in inputRDD.

    #         C1: parallelize             T1: keys
    # dataset -----------------> inputRDD ----------> keysRDD

    keysRDD = inputRDD.keys()

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: keys            A1: collect
    # dataset -----------------> inputRDD ---------> keysRDD ------------> resVAL

    resVAL = keysRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)


# ------------------------------------------
# FUNCTION my_values
# ------------------------------------------
def my_values(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, "A"), (1, "B"), (2, "B")] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([(1, "A"), (1, "B"), (2, "B")])

    # 2. Operation T1: Transformation 'keys', so as to get a new RDD with the keys in inputRDD.

    #         C1: parallelize             T1: values
    # dataset -----------------> inputRDD -----------> valuesRDD

    valuesRDD = inputRDD.values()

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: values              A1: collect
    # dataset -----------------> inputRDD -----------> valuesRDD ------------> resVAL

    resVAL = valuesRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    print("\n\n--- [BLOCK 1] keys ---")
    my_keys(sc)

    print("\n\n--- [BLOCK 2] values ---")
    my_values(sc)


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
