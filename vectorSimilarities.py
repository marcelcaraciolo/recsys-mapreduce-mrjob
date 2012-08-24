#-*-coding: utf-8 -*-

'''
    Given a dataset of ratings, how can we compute the similarity
    between pairs of items ?

    Subclasses from this module that provide a concrete implementation
    of the input (in the form of a tuple stream containing: a user, the
    item being rated, and the numeric rating of the item by the user)
    will calculate the similarities of items.

    Each item is represented as a (sparse) vector of all its ratings.
    Similarity measures (such as correlation, cosine and Jaccard) will
    be applied to these vectors.

'''
__author__ = 'Marcel Caraciolo <caraciol@gmail.com>'

from mrjob.job import MRJob
from math import sqrt

try:
    from itertools import combinations
except ImportError:
    def combinations(iterable, r):
        """
        Implementation of itertools combinations method.
         Re-implemented here because of import issues
          in Amazon Elastic MapReduce. Just easier to do this than bootstrap.
        More info here:
        http://docs.python.org/library/itertools.html#itertools.combinations

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


#Parameters to regularize correlation
PRIOR_COUNT = 10
PRIOR_CORRELATION = 0

#FILTERS to speed up computation and reduce noise
#Subclasses should probably override these, based on actual data.
MIN_NUM_RATERS = 2
MAX_NUM_RATERS = 10000
MIN_INTERSECTION = 0


class SemicolonValueProtocol(object):

    # don't need to implement read() since we aren't using it

    def write(self, key, values):
        return ';'.join(str(v) for v in values)


class VectorSimilarities(MRJob):

    OUTPUT_PROTOCOL = SemicolonValueProtocol

    def steps(self):
        return [
            self.mr(mapper=self.input, reducer=self.group_by_user_rating),
            self.mr(reducer=self.count_ratings_users_freq),
            self.mr(mapper=self.pairwise_items,
                    reducer=self.calculate_similarity),
            self.mr(mapper=self.calculate_ranking,
                    reducer=self.top_similar_items)]

    def configure_options(self):
        super(VectorSimilarities, self).configure_options()
        self.add_passthrough_option(
            '--priorcount', dest='prior_count', default=10, type='int',
            help='PRIOR_COUNT: Parameter to regularize correlation')

        self.add_passthrough_option(
            '--priorcorrelation', dest='prior_correlation', default=0,
             type='int',
             help='PRIOR_CORRELATION: Parameter to regularize correlation')

        self.add_passthrough_option(
            '--minraters', dest='min_num_raters', default=3, type='int',
            help='the minimum number of raters')

        self.add_passthrough_option(
            '--maxraters', dest='max_num_raters', default=10000, type='int',
            help='the maximum number of raters')

        self.add_passthrough_option(
            '--minintersec', dest='min_intersection', default=0, type='int',
            help='the minimum intersection')

    def input(self, key, line):
        '''
        Subclasses should override this to define their own input
        '''
        raise NotImplementedError('Implement this in the subclass')

    def group_by_user_rating(self, key, values):
        """
        Emit the user_id and group by their ratings (item and rating)

        17  70,3
        35  21,1
        49  19,2
        49  21,1
        49  70,4
        87  19,1
        87  21,2
        98  19,2

        """
        total = 0
        final = []
        for user_id, rating in values:
            total += 1
            final.append((user_id, rating))

        if total >= self.options.min_num_raters and \
           total <= self.options.max_num_raters:
            for user_id, rating in final:
                yield  user_id, (key, float(rating), total)
                #yield None, '%s;%s;%.2f;%d' % (user_id, key, rating, total)

    def count_ratings_users_freq(self, user_id, values):
        """
        For each user, emit a row containing their "postings"
        (item,rating pairs)
        Also emit user rating sum and count for use later steps.

        17    1,3,(70,3)
        35    1,1,(21,1)
        49    3,7,(19,2 21,1 70,4)
        87    2,3,(19,1 21,2)
        98    1,2,(19,2)
        """
        item_count = 0
        item_sum = 0
        final = []
        for item_id, rating, ratings_count in values:
            item_count += 1
            item_sum += rating
            final.append((item_id, rating, ratings_count))

        yield user_id, (item_count, item_sum, final)

    def pairwise_items(self, user_id, values):
        '''
        The output drops the user from the key entirely, instead it emits
        the pair of items as the key:

        19,21  2,1
        19,70  2,4
        21,70  1,4
        19,21  1,2

        This mapper is the main performance bottleneck.  One improvement
        would be to create a java Combiner to aggregate the
        outputs by key before writing to hdfs, another would be to use
        a vector format and SequenceFiles instead of streaming text
        for the matrix data.
        '''
        item_count, item_sum, ratings = values
        #print item_count, item_sum, [r for r in combinations(ratings, 2)]
        #bottleneck at combinations
        for item1, item2 in combinations(ratings, 2):
            yield (item1[0], item2[0]), \
                    (item1[1], item2[1], item1[2], item2[2])

    def calculate_similarity(self, pair_key, lines):
        '''
        Sum components of each corating pair across all users who rated both
        item x and item y, then calculate pairwise pearson similarity and
        corating counts.  The similarities are normalized to the [0,1] scale
        because we do a numerical sort.

        19,21   0.4,2
        21,19   0.4,2
        19,70   0.6,1
        70,19   0.6,1
        21,70   0.1,1
        70,21   0.1,1
        '''
        sum_xx, sum_xy, sum_yy, sum_x, sum_y, n = (0.0, 0.0, 0.0, 0.0, 0.0, 0)
        n_x, n_y = 0, 0
        item_pair, co_ratings = pair_key, lines
        item_xname, item_yname = item_pair
        for item_x, item_y, nx_count, ny_count in lines:
            sum_xx += item_x * item_x
            sum_yy += item_y * item_y
            sum_xy += item_x * item_y
            sum_y += item_y
            sum_x += item_x
            n += 1
            n_x = int(ny_count)
            n_y = int(nx_count)

        corr_sim = correlation(n, sum_xy, sum_x, \
                 sum_y, sum_xx, sum_yy)

        reg_corr_sim = regularized_correlation(n, sum_xy, sum_x, \
                sum_y, sum_xx, sum_yy, PRIOR_COUNT, PRIOR_CORRELATION)

        cos_sim = cosine(sum_xy, sqrt(sum_xx), sqrt(sum_yy))

        jaccard_sim = jaccard(n, n_x, n_y)

        yield (item_xname, item_yname), (corr_sim, \
                cos_sim, reg_corr_sim, jaccard_sim, n)

    def calculate_ranking(self, item_keys, values):
        '''
        Emit items with similarity in key for ranking:

        19,0.4    70,1
        19,0.6    21,2
        21,0.6    19,2
        21,0.9    70,1
        70,0.4    19,1
        70,0.9    21,1

        '''
        corr_sim, cos_sim, reg_corr_sim, jaccard_sim, n = values
        item_x, item_y = item_keys
        if int(n) > self.options.min_intersection:
            yield (item_x, corr_sim, cos_sim, reg_corr_sim, jaccard_sim), \
                     (item_y, n)

    def top_similar_items(self, key_sim, similar_ns):
        '''
        For each item emit K closest items in comma separated file:

        De La Soul;A Tribe Called Quest;0.6;1
        De La Soul;2Pac;0.4;2

        '''
        item_x, corr_sim, cos_sim, reg_corr_sim, jaccard_sim = key_sim
        for item_y, n in similar_ns:
            yield None, (item_x, item_y, corr_sim, cos_sim, reg_corr_sim,
                         jaccard_sim, n)


def correlation(size, dot_product, rating_sum, \
            rating2sum, rating_norm_squared, rating2_norm_squared):
    '''
      The correlation between two vectors A, B is
          [n * dotProduct(A, B) - sum(A) * sum(B)] /
        sqrt{ [n * norm(A)^2 - sum(A)^2] [n * norm(B)^2 - sum(B)^2] }

    '''
    numerator = size * dot_product - rating_sum * rating2sum
    denominator = sqrt(size * rating_norm_squared - rating_sum * rating_sum) * \
                    sqrt(size * rating2_norm_squared - rating2sum * rating2sum)

    return (numerator / (float(denominator))) if denominator else 0.0


def jaccard(users_in_common, total_users1, total_users2):
    '''
    The Jaccard Similarity between 2 two vectors
        |Intersection(A, B)| / |Union(A, B)|
    '''
    union = total_users1 + total_users2 - users_in_common

    return (users_in_common / (float(union))) if union else 0.0


def normalized_correlation(size, dot_product, rating_sum, \
            rating2sum, rating_norm_squared, rating2_norm_squared):
    '''
      The correlation between two vectors A, B is
      cov(A, B) / (stdDev(A) * stdDev(B))
      The normalization is to give the scale between [0,1].

    '''
    similarity = correlation(size, dot_product, rating_sum, \
            rating2sum, rating_norm_squared, rating2_norm_squared)

    return (similarity + 1.0) / 2.0


def cosine(dot_product, rating_norm_squared, rating2_norm_squared):
    '''
    The cosine between two vectors A, B
       dotProduct(A, B) / (norm(A) * norm(B))
    '''
    numerator = dot_product
    denominator = rating_norm_squared * rating2_norm_squared

    return (numerator / (float(denominator))) if denominator else 0.0


def regularized_correlation(size, dot_product, rating_sum, \
            rating2sum, rating_norm_squared, rating2_norm_squared,
            virtual_cont, prior_correlation):
    '''
    The Regularized Correlation between two vectors A, B

    RegularizedCorrelation = w * ActualCorrelation + (1 - w) * PriorCorrelation
        where w = # actualPairs / (# actualPairs + # virtualPairs).
    '''
    unregularizedCorrelation = correlation(size, dot_product, rating_sum, \
            rating2sum, rating_norm_squared, rating2_norm_squared)

    w = size / float(size + virtual_cont)

    return w * unregularizedCorrelation + (1.0 - w) * prior_correlation
