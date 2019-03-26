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
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, my_result_dir):
    # 1. Operation C1: Creation 'textFile', so as to store the content of the dataset into an RDD.

    # The dataset was previously loaded into DBFS by:
    # i) Place the desired dataset into your local file system folder %Test Environment%/my_dataset/
    # ii) Running the script "1.upload_dataset.bat"

    # Please note that, once again, the name textFile is a false friend here, as it seems that the parameter we are passing is the name of the file
    # we want to read. Indeed, the parameter here is the name of a folder, and the dataset consists on the content of the files of the folder.
    # An RDD of Strings is then generated, with one item per line of text in the files.
    # In this case, the RDD is distributed among different machines (nodes) of our cluster.

    #                            C1: textFile
    # dataset: DBFS inputFolder -------------> inputRDD

    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation A1: Action 'saveAsTextFile', so as to store the content of inputRDD into the desired DBFS folder.
    # If inputRDD is stored in 'm' nodes of our cluster, each node outputs a file 'part-XXXXX' to the DBFS outputFolder under a new file
    # parts-XXXXX (e.g., node 0 outputs the file part-00000, node 1 the file part-00001, and so on).
    # The whole content of the RDD being stored can be accesed by merging all output files.

    #                            C1: textFile           A1: saveAsTextFile
    # dataset: DBFS inputFolder -------------> inputRDD -------------------> DBFS outputFolder

    inputRDD.saveAsTextFile(my_result_dir)

    # 4. To access the result of DBFS outputFolder, we need to bring it back to our local machine by:
    #    i) Running the script "2.download_solution.bat", which places all part-XXXXX files into %Test Environment%/my_result/ and runs the
    #       Python program "3.merge_solutions.py", which merges all part-XXXXX files into the total solution file "solution.txt".


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Local or Databricks
    local_False_databricks_True = False

    # 2. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/1_Spark_Core/my_dataset/"
    my_result_dir = "FileStore/tables/1_Spark_Core/my_result/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
        my_result_dir = my_local_path + my_result_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir
        my_result_dir = my_databricks_path + my_result_dir

    # 3. We remove my_result directory
    if local_False_databricks_True == False:
        if os.path.exists(my_result_dir):
            shutil.rmtree(my_result_dir)
    else:
        dbutils.fs.rm(my_result_dir, True)

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, my_result_dir)
