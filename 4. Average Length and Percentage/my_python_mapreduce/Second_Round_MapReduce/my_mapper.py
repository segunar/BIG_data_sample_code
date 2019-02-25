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

import codecs
import sys

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = []

    # 2. We set the line to be split by " "
    line = line.replace("\n", "")
    line = line.strip()
    line = line.rstrip()
    line = line.replace("\t", " ")

    # 3. We get rid of chars not being either a letter or a " "
    index = len(line) - 1

    # 3.1. We traverse all characters
    while (index >= 0):
        # 3.1.1. We get the ord of the character at position index
        char_val = ord(line[index])

        # 3.1.2. If (1) char_val is not " " and (2) char_val is not an Upper Case letter and (3) char_val is not a Lower Case letter
        if ( ( char_val != 32) and ((char_val < 65) or (char_val > 90)) and ((char_val < 97) or (char_val > 122)) ):
            # 3.1.2.1. We remove the index from the sentence
            line = line[:index] + line[(index+1):]

        # 3.1.3. We continue with the next index
        index = index - 1

    # 4. We get the list of words
    res = line.split(" ")

    index = len(res) - 1

    # 4.1. We traverse the words
    while (index >= 0):
        # 4.1.1. If it is empty, we remove it
        if (res[index] == ''):
            del res[index]

        # 4.1.2. We continue with the next word
        index = index - 1

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(my_input_stream, my_output_stream, my_mapper_input_parameters):
    # 1. We unpack my_mapper_input_parameters
    pass

    # 2. We create two lists:

    # 2.1. One for counting the word appearances with starting letter
    num_words = [0] * 26

    # 2.2. One for counting the total_length of such these words
    total_length = [0] * 26

    # 3. We get the ordinal value for the letter 'a'
    letter_value = ord('a')

    # 4. We traverse the lines of the file
    for line in my_input_stream:
        # 4.1. We get the words from the line
        words = process_line(line)

        # 4.2. We traverse the words
        for word in words:
            # 4.2.1. We get the associated index for the first letter
            index = ord(word[0].lower()) - letter_value

            # 4.2.2. We update our lists
            num_words[index] = num_words[index] + 1
            total_length[index] = total_length[index] + len(word)

    # 5. We populate my_output_stream with the (key, value) pairs
    for index in range(26):
        # 5.1. We generate the String to be printed
        my_str = chr(letter_value) + '\t' + '(' + str(num_words[index]) + ',' + str(total_length[index]) + ')' + '\n'

        # 5.2. We print it
        my_output_stream.write(my_str)

        # 5.3. We update the letter
        letter_value = letter_value + 1

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(local_False_Cloudera_True,
            my_mapper_input_parameters,
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

    # 2. We trigger my_map
    my_map(my_input_stream, my_output_stream, my_mapper_input_parameters)

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
    input_file_example = "../../my_dataset/comedies.txt"
    output_file_example = "../../my_result/Second_Round_MapReduce/my_mapper_results.txt"

    # 3. my_mappper.py input parameters
    # We list the parameters here

    # We create a list with them all
    my_mapper_input_parameters = []

    # 4. We call to my_main
    my_main(local_False_Cloudera_True,
            my_mapper_input_parameters,
            input_file_example,
            output_file_example
           )
