import csv
# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
"""
The purpose of the following lines is to summarize useful
codes to load data from s3 and save a subsample that can
be later used to test the codes for analysis.

Remember that without these keys you cannot access s3:
export AWS_ACCESS_KEY_ID=<ACCESS_KEY_ID>
export AWS_SECRET_ACCESS_KEY=<SECRET_ACCESS_KEY>
Execute the above lines from command line, which means before entering the  shell
"""

#Load Data from S3
rdd = sc.sequenceFile("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data",
"org.apache.hadoop.io.Text",
"org.apache.hadoop.io.LongWritable")

#Take a Reasonable subsample to work with
data_sample = rdd.takeSample(False, 10000, seed=43)

# Write the Sample to the master
with open('Sample.csv','w') as f:
    writer = csv.writer(f, delimiter ='\t')
    writer.writerows(data_sample)

# Copy file from master to local
scp -i <key_file.pem> hadoop@ec2-aa-bb-cc-dd.compute-1.amazonaws.com:/home/hadoop/Sample.csv \
/home/sebastian/Documents/PySpark_Examples

# Read the File from local
with open('Sample.csv', 'rb') as csvfile:
    sample_data = []
    data = csv.reader(csvfile, delimiter ='\t')
    for row in data:
        sample_data.append(row)
        print ', '.join(row)
