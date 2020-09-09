# dataengineering
Project does data analysis of chicago crime data set available at https://www.kaggle.com/currie32/crimes-in-chicago?select=Chicago_Crimes_2008_to_2011.csv
It simulates stream of input data from csv file by using google template(https://cloud.google.com/dataflow/docs/guides/templates/provided-streaming#gcstexttocloudpubsubstream) and a dataflow job and also does batch processing using the same csv file.

Both stream_processing.py and batch_processing.py files need to be run on google cloud shell as it uses dataflow as its runner.

flaskapp is a simple flask app which just serves the screenshots of analysis done using bigquery and visualized using datastudio. The images goes into static folder.
