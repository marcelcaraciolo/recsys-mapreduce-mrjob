#-*-coding: utf-8 -*-

'''
Similarities measures

'''

__author__ = 'Marcel Caraciolo <caraciol@gmail.com>'


from math import sqrt


def correlation(size, dot_product, rating_sum, \
            rating2sum, rating_norm_squared, rating2_norm_squared):
    '''
      The correlation between two vectors A, B is
      cov(A, B) / (stdDev(A) * stdDev(B))

    '''
    numerator = size * dot_product - rating_sum * rating2sum
    denominator = sqrt(size * rating_norm_squared - rating_sum * rating_sum) * \
                    sqrt(size * rating2_norm_squared - rating2sum * rating2sum)

    return (numerator / (float(denominator))) if denominator else 0.0


def combinations(iterable, r):
    """
    Implementation of itertools combinations method. Re-implemented here because
    of import issues in Amazon Elastic MapReduce. Was just easier to do this than
    bootstrap.
    More info here: http://docs.python.org/library/itertools.html#itertools.combinations

    Input/Output:

    combinations('ABCD', 2) --> AB AC AD BC BD CD
    combinations(range(4), 3) --> 012 013 023 123
    """
    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    indices = range(r)
    yield tuple(pool[i] for i in indices)
    while True:
        for i in reversed(range(r)):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in range(i + 1, r):
            indices[j] = indices[j - 1] + 1
        yield tuple(pool[i] for i in indices)
