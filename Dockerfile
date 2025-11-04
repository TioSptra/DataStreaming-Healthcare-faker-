FROM cnfldemos/flink-kafka:1.19.1-scala_2.12-java17

WORKDIR /app

RUN apt-get update -y && \
apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev liblzma-dev && \
wget https://www.python.org/ftp/python/3.11.13/Python-3.11.13.tgz && \
tar -xvf Python-3.11.13.tgz && \
cd Python-3.11.13 && \
./configure --without-tests --enable-shared && \
make -j6 && \
make install && \
ldconfig /usr/local/lib && \
cd .. && rm -f Python-3.11.13.tgz && rm -rf Python-3.11.13 && \
ln -s /usr/local/bin/python3 /usr/local/bin/python && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

# install PyFlink
COPY requirements.txt .
RUN python -m pip install --upgrade pip; \
    pip3 install -r requirements.txt  --no-cache-dir;

# Download connector libraries
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-python/1.19.1/flink-python-1.19.1.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.19.1/flink-sql-connector-kafka-1.19.1.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/1.19.1/flink-connector-jdbc-1.19.1.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar;
    
WORKDIR /opt/flink