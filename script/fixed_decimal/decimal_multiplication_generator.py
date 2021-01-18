#!/usr/bin/env python3
import random


MAX_NUM_BITS = 38


def generate_random_num(precision):
    result = 0
    for j in range(precision):
        random_num = random.randint(0, 9)
        if j == 0 and random_num == 0:
            random_num = random_num + 1
        result += random_num
        if j != precision - 1:
            result *= 10
    return result


if __name__ == "__main__":
    random.seed(1)
    for i in range(10000000):

        precision = (i % MAX_NUM_BITS) + 1          # [1, MAX_NUM_BITS]

        di1 = generate_random_num(precision)
        di2 = generate_random_num(precision)

        result = str(di1 * di2)

        if len(result) == 2 * precision:
            result = '0.' + result[0:precision]
        else:
            result = '0.0' + result[0:precision - 1]

        print(f"0.{di1} {precision} 0.{di2} {precision} {result} {precision}")
