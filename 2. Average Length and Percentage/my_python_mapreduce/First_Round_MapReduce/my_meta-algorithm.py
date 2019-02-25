#!/usr/bin/python

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

import os
import codecs
import my_mapper
import my_reducer

# ------------------------------------------
# FUNCTION my_mapper_simulation
# ------------------------------------------
def my_mapper_simulation(input_directory, output_directory, my_mapper_input_parameters):
    # 1. We create the results file
    my_output_stream = codecs.open(output_directory + "my_mapper_results.txt", "w", encoding='utf-8')

    # 2. We collect the list of files we have to process
    file_names = os.listdir(input_directory)

    # 3. We process each file sequentially
    for file in file_names:
        # 3.1. We open the file to be read
        my_input_stream = codecs.open(input_directory + file, "r", encoding='utf-8')

        # 3.2. We process it
        my_mapper.my_map(my_input_stream, my_output_stream, my_mapper_input_parameters)

        # 3.3. We close the file
        my_input_stream.close()

    # 4. We close the results file
    my_output_stream.close()

# ------------------------------------------
# FUNCTION my_sort_simulation
# ------------------------------------------
def my_sort_simulation(output_directory):
    # 1. We create the results file
    my_output_stream = codecs.open(output_directory + "my_sort_results.txt", "w", encoding='utf-8')

    # 2. We open the source file
    my_input_stream = codecs.open(output_directory + "my_mapper_results.txt", "r", encoding='utf-8')

    # 3. We traverse the lines of the source file
    content = []
    for line in my_input_stream:
        # 3.1. We add the line's content to the list
        line = line.replace('\n', '')
        words = line.split('\t')
        content.append( (words[0], words[1]) )

    # 4. We sort the content
    content.sort()

    # 5. We print the sorted file to the output_stream
    for item in content:
        my_str = str(item[0]) + '\t' + str(item[1]) + '\n'
        my_output_stream.write(my_str)

    # 6. We close the file
    my_output_stream.close()

# ------------------------------------------
# FUNCTION my_reducer_simulation
# ------------------------------------------
def my_reducer_simulation(output_directory, my_reducer_input_parameters):
    # 1. We create the file we will write to
    my_output_stream = codecs.open(output_directory + "my_reducer_results.txt", "w", encoding='utf-8')

    # 2. We open the source file
    my_input_stream = codecs.open(output_directory + "my_sort_results.txt", "r", encoding='utf-8')

    # 3. We process it
    my_reducer.my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters)

    # 4. We close the source file
    my_input_stream.close()

    # 5. We close the file
    my_output_stream.close()

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(input_directory,
            output_directory,
            my_mapper_input_parameters,
            my_reducer_input_parameters,
            keep_tmp_files
           ):

    # 1. Map Stage: We simulate it by assuming that:
    # One my_mapper.py process is assigned to each file
    # All my_mapper.py processes are run sequentially
    # The results are written to the file my_mapper_results.txt
    my_mapper_simulation(input_directory, output_directory, my_mapper_input_parameters)

    # 2. Sort Stage: We simulate it by assuming that:
    # All results from my_map_simulation are written to the file my_mapper_results.txt
    # The results are written to the file my_sort_results.txt
    my_sort_simulation(output_directory)

    # 3. Reduce Stage: We simulate it by assuming that:
    # All results from my_sort_simulation are written to the file my_sort_results.txt
    # There is a single my_reducer.py process
    # The results are written to the file my_reducer_results.txt
    my_reducer_simulation(output_directory, my_reducer_input_parameters)

    # 4. If keep_tmp_files is False, we remove them
    if (keep_tmp_files == False):
        os.remove(output_directory + "my_mapper_results.txt")
        os.remove(output_directory + "my_sort_results.txt")


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Local or HDFS folders
    input_directory = "../../my_dataset/"
    output_directory = "../../my_result/First_Round_MapReduce/"

    # 2. my_mappper.py input parameters
    # We list the parameters here

    # We create a list with them all
    my_mapper_input_parameters = []

    # 3. my_reducer.py input parameters
    # We list the parameters here

    # We create a tuple with them all
    my_reducer_input_parameters = []

    # 4. We specify if we want verbose intermediate results to stay
    keep_tmp_files = False

    # 4. We call to my_main
    my_main(input_directory,
            output_directory,
            my_mapper_input_parameters,
            my_reducer_input_parameters,
            keep_tmp_files
           )
