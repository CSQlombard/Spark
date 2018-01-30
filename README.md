# Spark
Examples Codes for Machine Learning using Pyspark 

"multinomial_logistic_regression.py": Applies the classifier using Grid Search for parameter optimization, crossvalidation and makes predictions.

"Load_Save_sample_data_from_Big_Data.py":
Usefuls scripts to load/save data. It shows how to load data from s3 (google n-grams), take a sub-sample to work with later and save it to local.
The subsample used in the script is loaded in this folder as "Sample.csv" and contains 10.000 1-grams in english.
"Sample_big.csv" contains 100.000 1-grams in english.

"1_gram_analysis_git.py":
Loads 1-gram data from s3 and outputs words wich you might find interesting together with its year, number of occurrence, number of pages and books in which each appears. Saves the data in an output.csv file that can later be use with "plot_1_gram_data.py" to generate figures like "poverty_violence_love.pdf" https://github.com/CSQlombard/Spark/blob/master/poverty_violence_love.pdf and "Peace_and_War.pdf" https://github.com/CSQlombard/Spark/blob/master/Peace_and_War.pdf. 
Which show interesting correlations between the normalized occurence in time.

