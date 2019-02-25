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
    letter = words[0]
    value = words[1]

    # 4. As the value is a tuple, we extract its components too
    value = value.rstrip(')')
    value = value.strip('(')
    components = value.split(',')

    num_words = int(components[0])
    total_length = int(components[1])

    # 5. We assign res
    res = (letter, num_words, total_length)

    # 6. We return res
    return res

#---------------------------------------
#  FUNCTION print_key_value
#---------------------------------------
def print_key_value(letter, num_words, total_length, my_output_stream):
    # 1. We compute the average
    average = (total_length * 1.0) / (num_words * 1.0)

    # 2. We come up with the String to be printed
    my_str = letter + '\t' + str(average) + '\n'

    # 3. We print it
    my_output_stream.write(my_str)

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    # 1. We unpack my_mapper_input_parameters
    pass

    # 2. We create 3 variables:

    # 2.1. One for the current letter we are processing
    current_letter = ""

    # 2.2. One for the number of words found for it
    current_num_words = 0

    # 2.3. One for the total length of such these words
    current_total_length = 0

    # 3. We traverse the lines of the file
    for line in my_input_stream:
        # 3.1. We get the info from the line
        (new_letter, new_num_words, new_total_length) = get_key_value(line)

        # 3.2. If we are dealing with a new letter (key)
        if (new_letter != current_letter):
            # 3.2.1. If it is not the first letter we process, we print it
            if current_letter != "":
                print_key_value(current_letter, current_num_words, current_total_length, my_output_stream)

            # 3.2.2. We assign the current key to the new key we are starting processing
            current_letter = new_letter
            current_num_words = 0
            current_total_length = 0

        # 3.3. We update the num words and total letters
        current_num_words = current_num_words + new_num_words
        current_total_length = current_total_length + new_total_length

    # 4. We print the last letter
    if current_letter != "":
        print_key_value(current_letter, current_num_words, current_total_length, my_output_stream)

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
    my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters)

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
