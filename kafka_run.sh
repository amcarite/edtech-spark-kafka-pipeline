docker-compose up -d  #spin up the docker-compose containers. Make sure you are in the w205/project-2-amcarite directory

read -p "Pause Time 15 seconds" -t 15
echo "Continuing ...."

docker-compose exec kafka kafka-topics --list --zookeeper zookeeper:32181 #check the list of kafka topics 

read -p "Pause Time 5 seconds" -t 5
echo "Continuing ...."

docker-compose exec kafka kafka-topics --create --topic assessments --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181 #create a topic named "assessments" with 1 partition and 1 replication factor 

read -p "Pause Time 5 seconds" -t 5
echo "Continuing ...."

docker-compose exec kafka kafka-topics --list --zookeeper zookeeper:32181 #check the list of kafka topics now to see our assessments topic added

read -p "Pause Time 3 seconds" -t 3
echo "Continuing ...."

docker-compose exec mids bash -c "cat /w205/project-2-amcarite/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c | kafkacat -P -b kafka:29092 -t assessments" #take our json file from the directory and pipe it to jq for small data cleaning, removing one layer. then pipe it into Kafka to publish

read -p "Pause Time 5 seconds" -t 5
echo "Continuing ...."
echo "35.212.249.90" #IP address for easy copy/paste

docker-compose exec spark env PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port 7001 --ip 0.0.0.0 --allow-root --notebook-dir=/w205/' pyspark  #spin up pyspark notebook 

read p "Pausing Time 5 seconds" -t 5

