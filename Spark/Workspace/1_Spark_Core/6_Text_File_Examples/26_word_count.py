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
import shutil
import os

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line, bad_chars):
    # 1. We create the output variable
    res = []

    # 2. We clean the line by removing the bad characters
    for c in bad_chars:
        line = line.replace(c, '')

    # 3. We clean the line by removing each tabulator and set of white spaces
    line = line.replace('\t', ' ')
    line = line.replace('  ', ' ')
    line = line.replace('   ', ' ')
    line = line.replace('    ', ' ')

    # 4. We clean the line by removing any initial and final white spaces
    line = line.strip()
    line = line.rstrip()

    # 5. We split the line by words
    words = line.split(" ")

    # 6. We append each valid word to the list
    for word in words:
        if (word != ''):
            if ((ord(word[0]) > 57) or (ord(word[0]) < 48)):
                res.append(word)

    # 7. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, my_result_dir, bad_chars, n):
    # 1. Operation C1: Creation 'textFile', so as to store the content of the dataset contained in the folder dataset_dir into an RDD.
    # If the dataset is big enough, its content is to be distributed among multiples nodes of the cluster.
    # The operation reads the files of the folder line by line. Thus, each item of the RDD is going to be a String (the content of the line being read).

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---

    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: Transformation 'flatMap', so as to get a new RDD ('allWordsRDD') with all the words of inputRDD.

    # We apply now a lambda expression as F to bypass each item of the collection to our actual filtering function F2 requiring more than
    # one argument. The function F2 is process_line, which cleans the lines from all bad_chars and splits it into a list of words.
    # We apply flatMap instead of map as we are not interested in the words of each line, just the words in general.
    # Thus, map would have given us an RDD where each item had been a list of words, the list of words on each line (i.e., each item had been [String]).
    # On the other hand, flatMap allows us to flat the lists and get instead an RDD where each item is a String, a word of the dataset.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> all_wordsRDD     --- RDD items are String ---

    allWordsRDD = inputRDD.flatMap(lambda x: process_line(x, bad_chars))

    # 3. Operation T2: Transformation 'map', so as to get a new RDD (pairRDD) per word of the dataset.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> allWordsRDD     --- RDD items are String ---
    #                                       |
    #                                       | T2: map
    #                                       | ---------> pairWordsRDD     --- RDD items are (String, int) ---

    pairWordsRDD = allWordsRDD.map(lambda x: (x, 1))

    # 4. Operation T3: Transformation 'reduceByKey', so as to aggregate the amount of times each word appears in the dataset.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> allWordsRDD     --- RDD items are String ---
    #                                       |
    #                                       | T2: map
    #                                       | ---------> pairWordsRDD     --- RDD items are (String, int) ---
    #                                                    |
    #                                                    | T3: reduceByKey
    #                                                    |-----------------> solutionRDD    --- RDD items are (String, int) ---

    solutionRDD = pairWordsRDD.reduceByKey(lambda x, y: x + y)

    # 5. Operation S1: Store the RDD solutionRDD into the desired folder from the DBFS.
    # Each node containing part of solutionRDD will produce a file part-XXXXX with such this RDD subcontent, where XXXXX is the name of the node.
    # Besides that, if the writing operation is successful, a file with name _SUCCESS will be created as well.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> allWordsRDD     --- RDD items are String ---
    #                                       |
    #                                       | T2: map
    #                                       | ---------> pairWordsRDD     --- RDD items are (String, int) ---
    #                                                    |
    #                                                    | T3: reduceByKey
    #                                                    |-----------------> solutionRDD    --- RDD items are (String, int) ---
    #                                                                        |
    #                                                                        | S1: saveAsTextFile
    #                                                                        |--------------------> DBFS New Folder

    solutionRDD.saveAsTextFile(my_result_dir)

    # Extra: To debug the program execution, you might want to this three lines of code.
    # Each of them apply the action 'take', taking a few elements of each RDD being computed so as to display them by the screen.

    # resVAl = solutionRDD.take(10)
    # for item in resVAl:
    #  print(item)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    bad_chars = ['?', '!', '.', ',', ';', '_', '-', '\'', '|', '--', '(', ')', '[', ']', '{', '}', ':', '&', '\n']
    n = 30

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/1_Spark_Core/my_dataset/"
    my_result_dir = "FileStore/tables/1_Spark_Core/my_result"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
        my_result_dir = my_local_path + my_result_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir
        my_result_dir = my_databricks_path + my_result_dir

    # 4. We remove my_result directory
    if local_False_databricks_True == False:
        if os.path.exists(my_result_dir):
            shutil.rmtree(my_result_dir)
    else:
        dbutils.fs.rm(my_result_dir, True)

    # 5. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 6. We call to our main function
    my_main(sc, my_dataset_dir, my_result_dir, bad_chars, n)
