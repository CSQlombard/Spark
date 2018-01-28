import nltk
from nltk.stem.wordnet import WordNetLemmatizer
import csv

# Without these keys you cannot access s3.
# Run this from command line, which means before entering the pyspark shell

#export AWS_ACCESS_KEY_ID=<ACCESS_KEY_ID>
#export AWS_SECRET_ACCESS_KEY=<SECRET_ACCESS_KEY>

# Load Data from us-east-1 region
# It is recommended to build your cluster in this region.
# I generated a 1 Master, 5 Slaves m4.xlarge instances.
# Once the cluster is set up.
# The entire processing of the data should not take more than 20 minutes.

rdd2 = sc.sequenceFile("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data",
"org.apache.hadoop.io.Text",
"org.apache.hadoop.io.LongWritable", minSplits=1000)

""" To test the code with a samll dataset.

# Read the Sample File from local
with open('Sample_big.csv', 'rb') as csvfile:
    sample_data = []
    data = csv.reader(csvfile,delimiter='\t')
    for row in data:
        sample_data.append(row)

# Pre-process data
rdd2 = sc.parallelize(sample_data,100)
data_per_part = rdd2.mapPartitionsWithIndex(count_in_a_partition).collect()

"""

# Interesting words
int_words = ['globalization', 'capitalism', 'war','nuclear','love','hope','peace','inequality','poverty','chaos','violence','expressionism','dadaism']
#Keep only data from your int_words
rdd3 = rdd2.map(lambda x: keeper_func(x,int_words))
rdd4  = rdd3.filter(lambda x: x)
# Now the data should be managable and we can cache it
rdd4.cache()
# Super important, transforms tuples into key pairs.
rdd5 = rdd4.map(lambda x : (x[0], x[1:]))
# Group Everything By Key
total_data = rdd5.groupByKey().map(lambda x : (x[0], list(x[1]))).collect()

# Save data to File
myfile = open('output_28_01_2018.csv','w')
with myfile:
    writer = csv.writer(myfile)
    writer.writerows(total_data)

""" Functions """

def count_in_a_partition(idx, iterator):
  count = 0
  for _ in iterator:
    count += 1
  return idx, count

def keeper_func(x,int_words):
    out = []
    if len(x) != 2:
        out = []
    else:
        sep = x[1].split('\t')
        if len(sep) != 5:
            out = []
        else:
            try:
                word = sep[0].lower()
                if word in int_words:
                    out.append(word)
                    for item in sep[1:]:
                        out.append(item)
            except:
                pass
    final_out=[]
    if len(out) == 5:
        final_out = out
    return tuple(final_out) # VERY IMPORTANT!!
