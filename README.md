# Tracking User Activity

As a member of the data engineering team for an ed tech firm, mly project is to take a json file a large amount of data about online assessments and turn it into a usable datastructure for my data science team to use and run queries on the data 

# My tasks are:

Prepare the infrastructure to land the data in the form and structure it needs
to be to be queried.

- Publish and consume messages with Kafka
- Use Spark to transform the messages. 
- Use Spark to transform the messages so that you can land them in HDFS

# How to run the pipeline process

- To begin, make sure you you are in the correct working directory -> /w205/project-2-amcarite
- next run the following bash command to set up your kafka pipeline and set up a pyspark notebook instance 

```
bash kafka_run.sh
```
- allow the script to run, there are many pauses in the script to allow enough time for each command to run
- To understand more about the script, please read the commented code in the kafka_run.sh file 
- next, take the url that is output for you and open up the notebook in another tab. navigate to the amcarite-Project-2.ipynb
- Run the code blocks and read the comments that detail what is hapening and why each line of code is run 
- After all the code blocks have been run, save and close the notebook and return to the terminal in your GCP instance
- type ^C twice to end the pyspark instance in the terminal
- You may run the below command to see the final clean data structures in HDFS

```
docker-compose exec cloudera hadoop fs -ls /tmp/
```

- End the docker-compose containers with the following command

```
docker-compose down
```

## Files in Repo

- docker-compose.yml
  - docker-compose file with all software services needed to run this pipeline
- edtech_pipeline_analysis.ipynb
  - jupyter notebook with pyspark. Commands notated with additional information on overall pipeline 

---
  

