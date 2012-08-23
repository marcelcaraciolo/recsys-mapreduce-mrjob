#-*-coding: utf-8 -*-
import sys
from movies_count import MoviesCount
from moviesSimilarities import MoviesSimilarities

writer = open('teste.txt', 'w')

mr_job = MoviesCount(args=[sys.argv[1], '-r', 'emr'])

with mr_job.make_runner() as runner:
    runner.run()
    for line in runner.stream_output():
        key, value = mr_job.parse_output_line(line)
        writer.write(key.encode('utf-8') + '\n')

writer.close()

mr_job2 = MoviesSimilarities(args=['teste.txt', '--num-ec2-instances', '5', '-r', 'emr'])
#'--python-archive', 'Documents/Projects/articles/mapreduce/metrics.tar.gz'])

with mr_job2.make_runner() as runner:
    runner.run()
    for line in runner.stream_output():
        key, value = mr_job2.parse_output_line(line)
        print key
