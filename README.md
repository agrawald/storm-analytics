# scenario analytics

This application is based on Apache Storm to perform analytics. The application uses following tools and technologies

 - Apache Storm
 - Apache Zookeeper
 - CouchDB
 - RabbitMQ
 
# steps to deploy

Following docker commands is used to create a basic development env

1. docker run -p 5984:5984 --name couchdb -d couchdb

2. docker run -d --restart always --name zookeeper zookeeper

3. docker run -d --hostname my-rabbit --name rabbit byteflair/rabbitmq-stomp

4. docker run -d --restart always --name nimbus --link zookeeper:zookeeper --link couchdb:couchdb --link rabbit:rabbit storm storm nimbus

5. docker run -d --restart always --name supervisor --link zookeeper:zookeeper --link nimbus:nimbus --link couchdb:couchdb --link rabbit:rabbit storm storm supervisor

6. docker run -d -p 8080:8080 --restart always --name ui --link nimbus:nimbus storm storm ui

7. docker run --link nimbus:nimbus -it --rm -v c:/Users/dagrawal/Documents/WORKSPACE/poc-storm/target/poc-storm-1.0-SNAPSHOT-jar-with-dependencies.jar:/topology.jar storm storm jar /topology.jar au.dj.poc.storm.Application topology
