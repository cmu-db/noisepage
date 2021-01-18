#!/usr/bin/env python3

# Hacker's Delight [2E Chapter 10 Integer Division By Constants] describes
# an algorithm for fast unsigned integer division through the use of
# "magic numbers" that satisfy nice mathematical properties.
#
# Given divisors and word sizes, this script generates their corresponding
# magic numbers by implementing the Hacker's Delight algorithm.

def get_magic_number(divisor, word_size):
    """
    Given word size W >= 1 and divisor d, where 1 <= d < 2**W,
    finds the least integer m and integer p such that
        floor(mn // 2**p) == floor(n // d) for 0 <= n < 2**W
    with 0 <= m < 2**(W+1) and p >= W.

    Implements the algorithm described by Hacker's Delight [2E], specifically,
    section 10-9 Unsigned Division by Divisors >= 1.

    Parameters
    ----------
    divisor : int
        The divisor d in the problem statement.
    word_size : int
        The word size W in the problem statement. The number of bits.

    Returns
    -------
    M : hex
        The magic number.
    p : int
        The exponent p.
    algorithm_type : int
        Debug information. Valid values are 0 and 1. The case that was used.
    """
    d, W = divisor, word_size

    nc = (2**W // d) * d - 1                                # Eqn (24a)
    for p in range(2 * W + 1):                              # Eqn (29)
        if 2 ** p > nc * (d - 1 - (2 ** p - 1) % d):        # Eqn (27)
            m = (2 ** p + d - 1 - (2 ** p - 1) % d) // d    # Eqn (26)
            # Unsigned case, the magic number M is given by:
            #   m             if 0 <= m < 2**W
            #   m - 2**W      if 2**W <= m < 2**(W+1)
            if 0 <= m < 2**W:
                return m, p, 0
            elif 2**W <= m < 2**(W+1):
                return m - 2**W, p, 1
            else:
                raise RuntimeError("Invalid case. Not enough bits?")


if __name__ == "__main__":
    def print_output(magic_number_output):
        M, p, alg = magic_number_output
        print(f"Magic Number: {hex(M)}")
        print(f"p: {p}")
        print(f"Algorithm Type: {alg}")

    while True:
        divisor = input("Divisor (or QUIT to quit): ")
        if divisor.upper() == "QUIT":
            break
        divisor = int(divisor)
        print("Add these to \"execution/sql/decimal_magic_numbers.h\".")
        print("128-bit, magic_map128_bit_constant_division")
        print_output(get_magic_number(divisor, 128))
        print("256-bit, magic_map256_bit_constant_division")
        print_output(get_magic_number(divisor, 256))
        print()
