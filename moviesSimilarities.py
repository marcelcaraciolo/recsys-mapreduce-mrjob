#-*-coding: utf-8 -*-

'''
 Given a dataset of movies and their ratings by different
 users, how can we compute the similarity between pairs of
 movies?

 This module computes similarities between movies
 by representing each movie as a vector of ratings and
 computing similarity scores over these vectors.


'''
__author__ = 'Marcel Caraciolo <caraciol@gmail.com>'

from mrjob.job import MRJob
from metrics import  correlation
from metrics import cosine, regularized_correlation
from math import sqrt

try:
    from itertools import combinations
except ImportError:
    from metrics import combinations


PRIOR_COUNT = 10
PRIOR_CORRELATION = 0


class SemicolonValueProtocol(object):

    # don't need to implement read() since we aren't using it

    def write(self, key, values):
        return ';'.join(str(v) for v in values)


class MoviesSimilarities(MRJob):

    OUTPUT_PROTOCOL = SemicolonValueProtocol

    def steps(self):
        return [
            self.mr(mapper=self.group_by_user_rating,
                    reducer=self.count_ratings_users_freq),
            self.mr(mapper=self.pairwise_items,
                    reducer=self.calculate_similarity),
            self.mr(mapper=self.calculate_ranking,
                    reducer=self.top_similar_items)]

    def group_by_user_rating(self, key, line):
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
        user_id, item_id, rating = line.split('|')
        #yield (item_id, int(rating)), user_id
        #yield item_id, (user_id, int(rating))
        yield  user_id, (item_id, float(rating))
        #yield (user_id, item_id), int(rating)

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
        for item_id, rating in values:
            item_count += 1
            item_sum += rating
            final.append((item_id, rating))

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
                    (item1[1], item2[1])

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
        item_pair, co_ratings = pair_key, lines
        item_xname, item_yname = item_pair
        for item_x, item_y in lines:
            sum_xx += item_x * item_x
            sum_yy += item_y * item_y
            sum_xy += item_x * item_y
            sum_y += item_y
            sum_x += item_x
            n += 1

        corr_sim = correlation(n, sum_xy, sum_x, \
                 sum_y, sum_xx, sum_yy)

        reg_corr_sim = regularized_correlation(n, sum_xy, sum_x, \
                sum_y, sum_xx, sum_yy, PRIOR_COUNT, PRIOR_CORRELATION)

        cos_sim = cosine(sum_xy, sqrt(sum_xx), sqrt(sum_yy))

        jaccard_sim = 0.0

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
        if int(n) > 0:
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


if __name__ == '__main__':
    MoviesSimilarities.run()
