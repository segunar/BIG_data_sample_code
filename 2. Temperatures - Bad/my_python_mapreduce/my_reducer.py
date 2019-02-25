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

import sys
import codecs

#---------------------------------------
#  FUNCTION get_key_value
#---------------------------------------
def get_key_value(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line char
    line = line.replace('\n', '')

    # 3. We get the key and value
    words = line.split('\t')
    city = words[0]
    value = words[1]

    # 4. As the value is a tuple, we extract its components too
    value = value.rstrip(')')
    value = value.strip('(')
    components = value.split(',')

    year = int(components[0])
    temp = float(components[1])

    # 5. We assign res
    res = (city, year, temp)

    # 6. We return res
    return res

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    # 1. We unpack my_mapper_input_parameters
    pass

    # 2. We create 3 variables:

    # 2.1. The name of the city
    best_city = ""

    # 2.2. The best year
    best_year = 10000

    # 2.3. The best temp
    best_temp = 10000.0

    # 3. We read one by one the pairs key-value passed from the mapper
    for line in my_input_stream:
        # 1.1. We extract the key and the value
        (new_city, new_year, new_temp) = get_key_value(line)

        # 1.2. We compare the new temperature with the best we have so far. If it is better, we update it
        if (new_temp < best_temp):
            best_city = new_city
            best_year = new_year
            best_temp = new_temp

    # 4. We print the result
    my_str = best_city + '\t' + '(' + str(best_year) + ',' + str(best_temp) + ')' + '\n'
    my_output_stream.write(my_str)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(local_False_Cloudera_True,
            my_reducer_input_parameters,
            input_file_example,
            output_file_example
           ):

    # 1. We select the input and output streams based on our working mode
    my_input_stream = None
    my_output_stream = None

    # 1.1: Local Mode --> We use the debug files
    if (local_False_Cloudera_True == False):
        my_input_stream = codecs.open(input_file_example, "r", encoding='utf-8')
        my_output_stream = codecs.open(output_file_example, "w", encoding='utf-8')

    # 1.2: Cloudera --> We use the stdin and stdout streams
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # 2. We trigger my_reducer
    my_reducer(my_input_stream, my_output_stream, my_reducer_input_parameters)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Local Mode or Cloudera
    local_False_Cloudera_True = False

    # 2. Debug Names
    input_file_example = "../my_result/my_sort_results.txt"
    output_file_example = "../my_result/my_reducer_results.txt"

    # 3. my_reducer.py input parameters
    # We list the parameters here

    # We create a list with them all
    my_reducer_input_parameters = []

    # 4. We call to my_main
    my_main(local_False_Cloudera_True,
            my_reducer_input_parameters,
            input_file_example,
            output_file_example
           )
