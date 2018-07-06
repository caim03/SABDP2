# Project Title

Progetto 2 - Sistemi e Architetture per Big data

### Quickstart

Run for build all the containers:

```
sudo ./launchEnvironment.sh
```

this command will build a flink cluster, a rabbitmq broker, a producer (he reads the dataset and publish on a queue), a MongoDB database and a consumer for mongoDB


And for stop all the environment:
```
sudo ./stopEnvironment.sh
```


you can log into the producer, the mongocosumer and the flink master with this commands:

```
sudo ./logInFlinkMaster.sh
sudo ./logInProducer.sh
sudo ./logInMongoConsumer.sh
```

## Queries

The step for execute all the pipeline are:

0) Download the dataset and put it in producer/app/data (it is a shared folder, you can put it manually)

1) Log into the producer, then
```
cd /app
./startProducer
```
The producer will read the dataset and publish all lines in 3 different rabbitmq queues

2) Log into the mongoConsumer, then
```
cd /app
./startMongoConsumer
```

he will be listening on 9 different output rabbitmq queue, published by the Flink cluster

3) Log into the Flink Master
```
cd /app
./query1 or ./query2 or ./query3
```

the queries will start, and flink will publish the output into different rabbitmq queues

## Exposed ports
you can explore all components by accessing the exposed ports on localhost:


```
http://localhost:8080 - Apache Flink Web Dashboard
```
```
http://localhost:15672 - RabbitMQ Management
```
```
http://localhost:8081 - Mongo Express
```


## Built With

* [Apache Flink](https://flink.apache.org/) - Stateful Computations over Data Streams
* [MongoDB](https://www.mongodb.com/) - Document oriented Database
* [RabbitMQ](https://www.rabbitmq.com/) - Open source message broker
* [Docker-Compose](https://docs.docker.com/compose/) - Tool for defining and running multi-container Docker application
