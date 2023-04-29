from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class Top10MoviesByTitleLength(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.filter_by_rating),
            MRStep(mapper=self.join_titles,
                   mapper_init=self.read_titles,
                   reducer=self.compute_title_length),
            MRStep(reducer=self.find_top10_titles)
        ]

    def filter_by_rating(self, _, line):
        user_id, movie_id, rating, _ = line.split('\t')
        if int(rating) >= 10:
            yield movie_id, None

    def read_titles(self):
        with open('u.item', 'r', encoding='ISO-8859-1') as f:
            for line in f:
                fields = line.split('|')
                movie_id = fields[0]
                movie_title = fields[1]
                yield movie_id, movie_title

    def join_titles(self, movie_id, _):
        yield movie_id, None

    def compute_title_length(self, movie_id, _):
        if movie_id in self.titles_dict:
            movie_title = self.titles_dict[movie_id]
            title_length = len(movie_title)
            yield title_length, movie_title

    def find_top10_titles(self, title_length, movie_titles):
        for movie_title in movie_titles:
            if self.n < 10:
                self.n += 1
                yield None, movie_title

    def mapper_init(self):
        self.titles_dict = {}
        with open('u.item', 'r', encoding='ISO-8859-1') as f:
            for line in f:
                fields = line.split('|')
                movie_id = fields[0]
                movie_title = fields[1]
                self.titles_dict[movie_id] = movie_title

    def reducer_init(self):
        self.n = 0

if __name__ == '__main__':
    Top10MoviesByTitleLength.run()
