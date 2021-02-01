#!/usr/bin/env python3
import random


MAX_NUM_BITS = 38


def generate_random_num(scale):
    result = 0
    for j in range(scale):
        random_num = random.randint(0, 9)
        if j == 0 and random_num == 0:
            random_num = random_num + 1
        result += random_num
        if j != scale - 1:
            result *= 10
    return result


if __name__ == "__main__":
    random.seed(1)
    for i in range(10000000):

        scale = (i % MAX_NUM_BITS) + 1          # [1, MAX_NUM_BITS]

        di1 = generate_random_num(scale)
        di2 = generate_random_num(scale)

        result = str(di1 * di2)

        if len(result) == 2 * scale:
            result = '0.' + result[0:scale]
        else:
            result = '0.0' + result[0:scale - 1]

        print(f"0.{di1} {scale} 0.{di2} {scale} {result} {scale}")
