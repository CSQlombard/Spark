import csv
import numpy as np
import string
import nltk

# Without these keys you cannot access s3.
# Run this from command line, which means before entering the pyspark shell
#export AWS_ACCESS_KEY_ID=<ACCESS_KEY_ID>
#export AWS_SECRET_ACCESS_KEY=<SECRET_ACCESS_KEY>

# Load Data, should be 4.8 GB
# load from us-east-1 region

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
# Define a time vector
time_vector= range(1905,1950,1)
# IMPORTANT: I did not addapt the occurrence_ratio to
# accomodate a binning different han 1. Binning > 1 has to group the same years
# together as well as all the years that fall in the same bin.
# The last part is not taken care of.
#Keep only data belonging to time vector
nltk.download('stopwords') # Very important dont forget to download!
stopwords = nltk.corpus.stopwords.words('english')
rdd3 = rdd2.map(lambda x: keeper_func(x,stopwords,time_vector))
rdd4  = rdd3.filter(lambda x: x) # reduces data to 197.059.943, 67%
#Coalesce partitions to reduce computation and re order data
rdd5  = rdd4.coalesce(500)
# Super important, transforms tuples into key pairs
rdd6 = rdd5.map(lambda x : (x[0], x[1:]))
# GroupByKey
rdd7 = rdd6.groupByKey().map(lambda x : (x[0], list(x[1]))) # reduces data to 2.887.932, 1.52%
#rdd8  = rdd7.coalesce(48)
# Compute Occurrence ratio
THR = 1.2 # consider words that have more than THR mean occurrence_ratio
# 0 is the minimum possible value.
rdd8 = rdd7.map(lambda x: (x[0], occurrence_ratio(x,THR)))
rdd9 = rdd8.filter(lambda x: len(x[1])>0) # reduces data to 332.847
rdd9.cache()
# It would be nice to avoid this an apply the correlation with others rdd
# only once.
word = rdd9.filter(lambda x: x[0] == 'war').collect()
word = word[0]
# Compute correlation with word
rdd10 = rdd9.map(lambda x: (x,correlation(word,x,time_vector)))
rdd10.cache()
# Define Threshold for correlation
rdd11 = rdd10.filter(lambda x: x[1]>0.95).filter(lambda x: x[1])

""" Collect only if it has a reasonable size """
output_data = rdd11.collect()

myfile = open('output_01_02_2018_v1.csv','w')
with myfile:
    writer = csv.writer(myfile)
    writer.writerows(output_data)

""" Functions """

def count_in_a_partition(idx, iterator):
  count = 0
  for _ in iterator:
    count += 1
  return idx, count

def keeper_func(x,stopwords,time_vector):
    out = []
    if len(x) != 2:
        out = []
    else:
        sep = x[1].split('\t')
        if len(sep) != 5:
            out = []
        else:
            try:
                # Eliminate punctuation and stopwords
                if unicode(sep[0].lower()) not in stopwords and sep[0].lower() not in string.punctuation:
                    if int(sep[1]) > time_vector[0] and int(sep[1]) < time_vector[-1]:
                        word = sep[0].lower()
                        out.append(word)
                        for item in sep[1:]:
                            out.append(item)
            except:
                pass
    final_out=[]
    if len(out) == 5:
        final_out = out
    return tuple(final_out) # VERY IMPORTANT!!

def occurrence_ratio(x,THR):
    oc_dict = {}
    values = x[1]
    for item in values:
        if item[0] not in oc_dict.keys():
            oc_dict[item[0]] = (int(item[1]), int(item[2]))
        else:
            oc_dict[item[0]] = (oc_dict[item[0]][0]+int(item[1]), oc_dict[item[0]][1]+int(item[2]))
    out = []
    total_value = 0
    for key in oc_dict.keys():
        value = float(oc_dict[key][0])/float(oc_dict[key][1])
        total_value = total_value + value
        out.append((key,value))
    # Consider only words that have interesting occur ratio
    out_final = []
    if total_value/len(out) >= THR:
        out_final = out
    return out_final

def correlation(word,x,time_vector):
    count_word = np.zeros([1,len(time_vector)-1])
    count_x = np.zeros([1,len(time_vector)-1])
    count1 = 0
    count2 = 0
    # convert to vector
    for j,_ in enumerate(word[1]):
        year = word[1][j][0]
        for i,time in enumerate(time_vector[:-1]):
            if int(year) > int(time_vector[i]) and int(year) <= int(time_vector[i+1]):
                count_word[0,i] = float(word[1][j][1])
                count1 +=1
    for j,_ in enumerate(x[1]):
        year = x[1][j][0]
        for i,time in enumerate(time_vector[:-1]):
            if int(year) > int(time_vector[i]) and int(year) <= int(time_vector[i+1]):
                count_x[0,i] = float(x[1][j][1])
                count2 +=1
    coeff = []
    if count1 > 0 and count2 > 0:
        value = np.corrcoef(count_word,count_x)
        coeff = value[0][1]
    return coeff
