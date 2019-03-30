
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

# ------------------------------------------
# 1. POLYMORPHISM
# ------------------------------------------
#
# ------------------------------------------
# FUNCTION fst
# ------------------------------------------
def fst(x, y):
    return x

# ------------------------------------------
# FUNCTION snd
# ------------------------------------------
def snd(x, y):
    return y

# ------------------------------------------
# FUNCTION my_take
# ------------------------------------------
def my_take(n, my_list):
    # 1. We create the output variable
    res = []

    # 2. We also get the length of the list
    size = len(my_list)

    # 3. We traverse as many elements as requested
    index = 0
    while (index < size):
        # 3.1. If the index is to be taken
        if (index < n):
            # 3.1.1. We append it to the result
            res.append(my_list[index])
            # 3.1.2. We continue by exploring the next index
            index = index + 1
        # 3.2. If the index is not to be taken
        else:
            # 3.2.1. We conclude the loop
            index = size

    # 4. We return res
    return res

# ------------------------------------------
# 2. HIGHER ORDER FUNCTIONS
# ------------------------------------------

# ------------------------------------------
# FUNCTION my_add
# ------------------------------------------
# def my_add(x, y):
#     # 1. We create the variable to output
#     res = x + y

#     # 2. We return res
#     return res

# ------------------------------------------
# FUNCTION add_one
# ------------------------------------------
def add_one(y):
    # 1. We create the variable to output
    res = y + 1

    # 2. We return res
    return res

# ------------------------------------------
# FUNCTION add_nine
# ------------------------------------------
def add_nine(x):
    # 1. We create the variable to output
    res = x + 9

    # 2. We return res
    return res


# ------------------------------------------
# FUNCTION my_mult
# ------------------------------------------
def my_mult(x, y):
    # 1. We create the variable to output
    res = x * y

    # 2. We return res
    return res

# ------------------------------------------
# FUNCTION bigger_than_three
# ------------------------------------------
def bigger_than_three(x):
    # 1. We create the variable to output
    res = False

    # 2. If the element is bigger than 3 we make it to pass the test
    if (x > 3):
        res = True

    # 2. We return res
    return res

# ------------------------------------------
# FUNCTION my_bigger
# ------------------------------------------
def my_bigger(x, y):
    # 1. We create the variable to output
    res = False

    # 2. If the element is bigger than 3 we make it to pass the test
    if (x < y):
        res = True

    # 3. We return res
    return res

# ------------------------------------------
# FUNCTION my_equal
# ------------------------------------------
def my_equal(x, y):
    # 1. We create the variable to output
    res = False

    # 2. If the element is bigger than 3 we make it to pass the test
    if (x == y):
        res = True

    # 3. We return res
    return res

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
# def my_map(funct, my_list):
#     # 1. We create the output variable
#     res = []

#     # 2. We populate the list with the higher application
#     for item in my_list:
#         sol = funct(item)
#         res.append(sol)

#     # 3. We return res
#     return res

# ------------------------------------------
# FUNCTION my_filter
# ------------------------------------------
def my_filter(funct, my_list):
    # 1. We create the output variable
    res = []

    # 2. We populate the list with the higher application
    for item in my_list:
        # 2.1. If an item satisfies the function, then it passes the filter
        if funct(item) == True:
            res.append(item)

    # 3. We return res
    return res

# ------------------------------------------
# FUNCTION my_fold
# ------------------------------------------
def my_fold(funct, accum, my_list):
    # 1. We create the output variable
    res = accum

    # 2. We populate the list with the higher application
    for item in my_list:
        res = res + funct(accum, item)

    # 3. We return res
    return res

# ------------------------------------------
#
# 3. PARTIAL APPLICATION
#
# ------------------------------------------

# SAME FUNCTIONS AS ABOVE

# ------------------------------------------
#
# 4. LAZY EVALUATION
#
# ------------------------------------------

# ------------------------------------------
# FUNCTION loop
# ------------------------------------------
def loop():
    # 1. We create the variable to output
    #res = loop()

    x = 0
    while (x < 1):
        pass

    # 2. We return loop
    return res

# ------------------------------------------
# FUNCTION from_n
# ------------------------------------------
def from_n(n):
    # 1. We create the variable to output
    res = [n]

    # 2. We recursively compute res
    res = res + from_n(n+1)

    # 3. We return res
    return res

# ------------------------------------------
# FUNCTION n_first_primes
# ------------------------------------------
def n_first_primes(n):
    # 1. We generate the variable to output
    res = []

    # 2. We use a couple of extra variables
    candidate = 2
    primes_found = 0

    # 3. We foundd the first n prime numbers
    while (primes_found < n):

        # 3.1. We traverse all numbers in [2..(candidate-1)] to find if there is any divisor
        index = 2
        divisor_found = False

        while (index < candidate) and (divisor_found == False):
            # 3.1.1. If the number is a divisor, we stop
            if (candidate % index) == 0:
                divisor_found = True
            # 3.1.2. If it is not, we continue with the next number
            else:
                index = index + 1

        # 3.2. If the number is a prime
        if divisor_found == False:
            # 3.2.1. We include the prime number in the list
            res.append(candidate)
            # 3.2.2. We increase the number of prime numbers being found
            primes_found = primes_found + 1

        # 3.3. We increase candidate, to test the next integer as prime number
        candidate = candidate + 1

    # 4. We return res
    return res

# ------------------------------------------
# FUNCTION n_first_long_lines
# ------------------------------------------
def n_first_long_lines(longitude, required_lines, file_name):
    # 1. We create the variable to output
    res = []

    # 2. We open the file for reading
    my_input_file = codecs.open(file_name, "r", encoding="utf-8")

    # 3. We create extra variables for the lines being read
    good_lines = 0

    # 4. We traverse the lines of the file until we find the desired ones
    if good_lines < required_lines:
        for line in my_input_file:
            # 4.1. If the line has the right length
            if len(line) > longitude:
                # 4.1.1. We include the line
                res.append(line)

                # 4.1.2. We increase the number of read lines
                good_lines = good_lines + 1

                # 4.1.3. We check if we have reached the required amount of lines
                if (good_lines == required_lines):
                    # 4.1.3.1. We end up the entire loop
                    break

    # 5. We close the file
    my_input_file.close()

    # 6. We return res
    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(option):
    # 1. We create the output variable
    res = None

    #---------------------------------
    # We run the desired option
    # ---------------------------------

    # ----------------------------------
    # 1. POLYMORPHISM
    # ----------------------------------

    #---------------------------------
    # 1.1. Basic Polymorphism
    #---------------------------------
    if option == 11:
        res = fst(3, True)
    if option == 12:
        res = fst(True, 3)

    #---------------------------------
    # 1.2. Take Elements from a List
    #---------------------------------
    if option == 13:
        res = my_take(3, [1,3,5,7,9])
    if option == 14:
        res = my_take(3, [True, False, True, False, False])
    if option == 15:
        res = my_take(1, [1,3,5,7,9])
    if option == 16:
        res = my_take(0, [1,3,5,7,9])
    if option == 17:
        res = my_take(10, [1,3,5,7,9])

    #---------------------------------
    # 2. HIGHER ORDER FUNCTIONS
    #---------------------------------

    #---------------------------------
    # 2.1. Basic functions
    #---------------------------------
    if option == 21:
        res = my_map(add_one, [1,2,3])
    if option == 22:
        res = my_filter(bigger_than_three, [1,5,2,7])
    if option == 23:
        res = my_fold(my_add, 0, [1,2,3,4])

    def my_map(funct, my_list):
        # 1. We create the output variable
        res = []

        # 2. We populate the list with the higher application
        for item in my_list:
            sol = funct(item)
            res.append(sol)

        # 3. We return res
        return res

    def my_add(x, y):
        # 1. We create the variable to output
        res = x + y

        # 2. We return res
        return res

    #---------------------------------
    # 3. PARTIAL APPLICATION
    #---------------------------------
    if option == 31:
        res = my_map(lambda y : my_add(1,y), [1,2,3])
    if option == 32:
        res = my_map(lambda y : my_add(10000,y), [1,2,3])
    if option == 33:
        res = my_filter(lambda y : my_bigger(3,y), [1,5,2,7])
    if option == 34:
        res = my_filter(lambda y : my_equal(4,y), [1,5,4,2,7,4])

    #---------------------------------
    # 4. LAZY EVALUATION
    #---------------------------------
    if option == 41:
        res = loop()
    if option == 42:
        res = fst(3, loop())
    if option == 43:
        res = from_n(1)

    # n primes with non-lazy
    if option == 44:
        res = n_first_primes(10)

    # Filter the first n lines with more than w words from a file
    if option == 45:
        res = n_first_long_lines(20, 2, "comedies.txt")

    # We return res
    return res

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. If we run it from command line
    if len(sys.argv) > 1:
        res = my_main(int(sys.argv[1]))
    # 2. If we run it via Pycharm
    else:
        option = 31
        res = my_main(option)

    # 3. We print the return value
    print(res)
