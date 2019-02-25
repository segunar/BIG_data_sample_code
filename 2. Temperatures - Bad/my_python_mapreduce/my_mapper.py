#!/usr/bin/python

# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Pythozoon interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs

#---------------------------------------
#  FUNCTION is_a_int
#---------------------------------------
def is_a_int(value):
    try:
        int(value)
        return True

    except ValueError:
        return False

#---------------------------------------
#  FUNCTION is_a_float
#---------------------------------------
def is_a_float(value):
    try:
        float(value)
        return True

    except ValueError:
        return False

#---------------------------------------
#  FUNCTION process_first_line
#---------------------------------------
def process_first_line(line):
    # 1. We create the output variable
    res = ""

    # 2. We get the info from the line
    line = line.replace('\n', '')
    line = line.strip()
    line = line.rstrip()
    words = line.split(' ')

    res = words[0]

    # 3. We return res
    return res

# ------------------------------------------
# FUNCTION process_year_line
# ------------------------------------------
def process_year_line(line):
    # 1. We create the output variable
    res = ()

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

        # 3.1.2. If (1) char_val is not " " and
        #           (2) char_val is not "-" and
        #           (3) char_val is not "." and
        #           (4) char_val is not "#" and
        #           (5) char_val is not a digit
        if ( (char_val != 32) and
             (char_val != 45) and
             (char_val != 46) and
             (char_val != 35) and
             ((char_val < 48) or (char_val > 57)) ):
            # 3.1.2.1. We remove the index from the sentence
            line = line[:index] + line[(index+1):]

        # 3.1.3. We continue with the next index
        index = index - 1

    # 4. We get the list of words
    words = line.split(" ")

    index = len(words) - 1

    # 4.1. We traverse the words
    while (index >= 0):
        # 4.1.1. If it is empty, we remove it
        if (words[index] == ''):
            del words[index]

        # 4.1.2. We continue with the next word
        index = index - 1

    # 5. We collect the year and the temperature

    # 5.1. If the line has enough fields, we process them.
    if (len(words) >= 4):
        # 5.1.1. We collect the fields
        year = words[0]
        temp = words[3]

        # 5.1.2 We check whether the fields have the right datatype
        if ((is_a_int(year) == True) and (is_a_float(temp) == True)):
            year = int(year)
            temp = float(temp)
        # 5.1.3. Otherwise, we return dummy values
        else:
            year = 10000
            temp = 10000.0
    # 5.2. Otherwise, we return dummy values
    else:
        year = 10000
        temp = 10000.0

    # 6. We assign res
    res = (year, temp)

    # 7. We return res
    return res

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(my_input_stream, my_output_stream, my_mapper_input_parameters):
    # 1. We unpack my_mapper_input_parameters
    pass

    # 2. We create three variables:

    # 2.1. The name of the city
    city = ""

    # 2.2. The best year
    year = 10000

    # 2.3. The best temp
    temp = 10000.0

    # 3. We process the first line
    line = my_input_stream.readline()
    city = process_first_line(line)

    # 4. We discard next 6 lines
    for iteration in range(6):
        my_input_stream.readline()

    # 5. We process the rest of the file
    for line in my_input_stream:
        # 5.1. We process the line
        (new_year, new_temp) = process_year_line(line)

        # 5.2. We update temp and year if needed
        if (new_temp < temp):
            temp = new_temp
            year = new_year

    # 6. We print the key value
    my_str = city + '\t' + '(' + str(year) + ',' + str(temp) + ')' + '\n'
    my_output_stream.write(my_str)

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
    print(ord(" "))
    print(ord("-"))
    print(ord("."))
    print(ord("#"))

    # 1. Local Mode or Cloudera
    local_False_Cloudera_True = False

    # 2. Debug Names
    input_file_example = "../my_dataset/Aberporth.txt"
    output_file_example = "../my_result/my_mapper_results.txt"

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
