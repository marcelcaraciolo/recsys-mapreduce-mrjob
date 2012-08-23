#-*-coding: utf-8 -*-
from vectorSimilarities import BookSimilarities
import sys

mr_job2 = BookSimilarities(args=[sys.argv[1],
            '--num-ec2-instances', '10', '-r', 'emr'])
with mr_job2.make_runner() as runner:
    runner.run()
    for line in runner.stream_output():
        key, value = mr_job2.parse_output_line(line)
        print key.encode('utf-8')
