# StreamAndBatchProcessing

---
## macOS/Linux
brew install temurin11 docker sbt
pip install kafka-python

## clone repo
git clone https://github.com/lucy-04/StreamAndBatchProcessing.git
cd StreamAndBatchProcessing

## start kafka and zookeeper
docker-compose up -d
docker ps  # ✅ Kafka & Zookeeper healthy

## start data generator
python data_gen.py

## start spark streaming
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
sbt "runMain StreamProcessor"

## start spark batch
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
sbt "runMain BatchProcessor"