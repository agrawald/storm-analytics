FROM storm
COPY ./target/analytics-1.0-SNAPSHOT-jar-with-dependencies.jar /topology.jar
WORKDIR /
CMD ["storm", "jar", "/topology.jar", "au.uni.melb.cloud.computing.analytics.Application", "-c", "nimbus.seeds=['115.146.86.204']", "-c", "nimbus.thrift.port=6627"]