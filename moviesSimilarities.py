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


class MoviesSimilarities(MRJob):

	def mapper(self, key, line):
		"""
		Mapper: send score from a single movie to
		other movies
		"""
		for user_id, item_id, rating in line.split(';'):
			print user_id, item_id, rating
		
	def reducer(self, _, values):
		pass


if __name__ == '__main__':
    MoviesSimilarities.run()
