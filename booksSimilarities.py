
from vectorSimilarities import VectorSimilarities


class BookSimilarities(VectorSimilarities):

    def input(self, key, line):
        user_id, item_id, rating = line.split(';')
        yield item_id, (user_id, float(rating))

if __name__ == '__main__':
    BookSimilarities.run()
