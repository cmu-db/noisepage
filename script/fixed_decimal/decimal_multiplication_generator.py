import math

from random import seed
from random import randint

# seed random number generator

seed(1)

# generate some integers

for i in range(10000000):

    precision = i % 38
    precision += 1
    d1 = '0.'
    d2 = '0.'

    di1 = 0
    di2 = 0

    for j in range(precision):
        random_num = randint(0, 9)
        if j == 0 and random_num == 0:
            random_num = random_num + 1
        di1 += random_num
        if j != precision - 1:
            di1 *= 10

    for j in range(precision):
        random_num = randint(0, 9)
        if j == 0 and random_num == 0:
            random_num = random_num + 1
        di2 += random_num

        if j != precision - 1:
            di2 *= 10

    d1 = d1 + str(di1)
    d2 = d2 + str(di2)

    stresult = str(di1 * di2)

    if len(stresult) == 2 * precision:
        stresult = '0.' + stresult[0:precision]
    else:
        stresult = '0.0' + stresult[0:precision - 1]

    print d1 + ' ' + str(precision) + ' ' + d2 + ' ' + str(precision) \
          + ' ' + stresult + ' ' + str(precision)