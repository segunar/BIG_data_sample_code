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
# FUNCTION process_word
# ------------------------------------------
def process_word(word):
    # 1. We create the output variable
    res = ()

    # 2. We create a couple of extra variables
    val1 = word[0].lower()
    val2 = len(word)

    # 3. We assign res properly
    res = (val1, val2)

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, my_result_dir, bad_chars):
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

    # 3. Operation T2: Transformation 'map', so as to get a new RDD ('pairWordsRDD') with a pair (letter, length of word) per word of the dataset.

    # Having the entire word for each word of the dataset is useless for us. Indeed, what we are interested into is just:
    # - The first letter (as we are outputting the average length per letter of the alphabet in the solution later on).
    # - The length of the word itself (as it is useful to us to compute such this average size).
    # Also, moving from an RDD it single String items into one of pairs (key, value) with key the letter and value the length of the word seems to be
    # a step in the right direction, as later one we can manipulate this (key, value) pairs with some of the pair RDD transformations and actions.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> allWordsRDD     --- RDD items are String ---
    #                                       |
    #                                       | T2: map
    #                                       | ---------> pairWordsRDD     --- RDD items are (char, int) ---

    pairWordsRDD = allWordsRDD.map(process_word)

    # 4. Operation T3: Transformation 'combineByKey', so as to get a new RDD ('letterTotalInfo') with a pair (letter, (num_letters, num_words)) per
    # letter of the alphabet.

    # The transformation operation 'combineByKey' requires as arguments 3 functions:

    # F1: To be applied in parallel to each node of the cluster.
    # The function is responsible of answering this question:
    # How do you want Spark to process the first (key, value) pair for each key k ?

    # If a node contains 1000 entries (key, value) with key 'k', F1 will only be applied once, for the first (key, value) found.
    # F1 must receive as input 1 parameter: The value of the (key, value) pair.
    # F1 must produce as an output 1 parameter: The accumulator accum generated for the pair (key, accum), created after
    # processing the first (key, value).

    # F2: To be applied in parallel to each node of the cluster.
    # The function is responsible of answering this question:
    # How do you want Spark to process all (key, value) pairs for each key k after having processed the first one and have got an accumulator ?

    # If a node contains 1000 entries (key, value) with key 'k', F2 will be applied 999 times, for all except the first (key, value) found.
    # F2 must receive as input 2 parameters:
    # - The accumulor generated until now.
    # - The value of the new (key, value) pair being found.
    # F2 must produce as an output 1 parameter: The updated accumulator, after aggregating it with the new (key, value) being found.

    # F3: To be applied as a whole single process through all nodes of the cluster.
    # The function is responsible of answering this question:
    # How do you want Spark to process all (key, accumulator) pairs so as to get a whole single (key, accumulator) pair ?

    # If combineByKey is applied to n nodes, F3 will be applied n-1 times, to merge all accumulators under a single accumulator.
    # F3 must receive as input 2 parameters:
    # - The meta-accumulor generated until now.
    # - The accumulator generated by node i, being processed now.
    # F3 must produce as an output 1 parameter: The updated accumulator, after aggregating it with the new (key, accumulator) being found.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> allWordsRDD     --- RDD items are String ---
    #                                       |
    #                                       | T2: map
    #                                       | ---------> pairWordsRDD     --- RDD items are (char, int) ---
    #                                                    |
    #                                                    | T3: combineByKey
    #                                                    |-------------------> letterTotalInfoRDD    --- RDD items are (char, (int, int))

    letterTotalInfoRDD = pairWordsRDD.combineByKey(lambda x: (x, 1),
                                                   lambda x, y: (x[0] + y, x[1] + 1),
                                                   lambda x, y: (x[0] + y[0], x[1] + y[1])
                                                   )

    # 5. Operation T4: Transformation 'mapValues', so as to get the average for letter in a new RDD ('solutionRDD').

    # We are nearly there. combineByKey has given us pretty much the results we needed. All it is left is to pass from the current accumulator per key
    # (letter, (num_letters, num_words)) to a new simple accumulator (letter, num_letters / num_words)
    # As we are not going to modify the key of each (key, value) pair, we apply the function mapValues instead of map.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> allWordsRDD     --- RDD items are String ---
    #                                       |
    #                                       | T2: map
    #                                       | ---------> pairWordsRDD     --- RDD items are (char, int) ---
    #                                                    |
    #                                                    | T3: combineByKey
    #                                                    |-------------------> letterTotalInfoRDD    --- RDD items are (char, (int, int))
    #                                                                          |
    #                                                                          | T4: mapValues
    #                                                                          |----------------> solutionRDD    --- RDD items are (char, float) ---

    solutionRDD = letterTotalInfoRDD.mapValues(lambda value: (value[0] * 1.0) / (value[1] * 1.0))

    # 6. Operation S1: Store the RDD solutionRDD into the desired folder from the DBFS.
    # Each node containing part of solutionRDD will produce a file part-XXXXX with such this RDD subcontent, where XXXXX is the name of the node.
    # Besides that, if the writing operation is successful, a file with name _SUCCESS will be created as well.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> allWordsRDD     --- RDD items are String ---
    #                                       |
    #                                       | T2: map
    #                                       | ---------> pairWordsRDD     --- RDD items are (char, int) ---
    #                                                    |
    #                                                    | T3: combineByKey
    #                                                    |-------------------> letterTotalInfoRDD    --- RDD items are (char, (int, int))
    #                                                                          |
    #                                                                          | T4: mapValues
    #                                                                          |----------------> solutionRDD    --- RDD items are (char, float) ---
    #                                                                                             |
    #                                                                                             | S1: saveAsTextFile
    #                                                                                             |--------------------> DBFS New Folder

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
    my_main(sc, my_dataset_dir, my_result_dir, bad_chars)
