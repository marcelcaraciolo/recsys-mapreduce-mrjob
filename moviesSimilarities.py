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
from metrics import correlation, normalized_correlation
try:
    from itertools import combinations
except ImportError:
    from metrics import combinations


class MoviesSimilarities(MRJob):

    def steps(self):
        return [self.mr(self.group_by_user_rating,
                         self.count_ratings_users_freq),
                self.mr(self.pairwise_items, self.calculate_similarity),
                self.mr(self.calculate_ranking, self.top_similar_items)
                ]

    def group_by_user_rating(self, key, line):
        """
        Mapper: send score from a single movie to
        other movies
        """
        user_id, item_id, rating = line.split('|')
        #yield (item_id, int(rating)), user_id
        #yield item_id, (user_id, int(rating))
        yield  user_id, (item_id, float(rating))
        #yield (user_id, item_id), int(rating)

    def count_ratings_users_freq(self, user_id, values):
        item_count = 0
        item_sum = 0
        final = []
        for item_id, rating in values:
            item_count += 1
            item_sum += rating
            final.append((item_id, rating))

        yield user_id, (item_count, item_sum, final)

    def pairwise_items(self, user_id, values):
        item_count, item_sum, ratings = values
        #print item_count, item_sum, [r for r in combinations(ratings, 2)]
        #bottleneck at combinations
        for item1, item2 in combinations(ratings, 2):
            yield (item1[0], item2[0]), \
                    (item1[1], item2[1])

    def calculate_similarity(self, pair_key, lines):
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
        similarity = normalized_correlation(n, sum_xy, sum_x, sum_y, \
                sum_xx, sum_yy)
        yield (item_xname, item_yname), (similarity, n)

    def calculate_ranking(self, item_keys, values):
        similarity, n = values
        item_x, item_y = item_keys
        if int(n) > 0:
            yield (item_x, similarity), (item_y, n)

    def top_similar_items(self, key_sim, similar_ns):
        item_x, similarity = key_sim
        for item_y, n in similar_ns:
            print '%s;%s;%f;%d' % (item_x, item_y, similarity, n)

if __name__ == '__main__':
    MoviesSimilarities.run()
