FROM centos:7

# install base modules
RUN yum -y update; \
    yum -y install epel-release; \    
    yum -y install wget sudo vim which; \
    echo 'alias vi="vim"' > /etc/profile.d/alias.sh

# setup user
ENV USER=hadoop
RUN useradd ${USER}; \
    echo "${USER}:${USER}" | chpasswd

# setup sudoers
RUN echo "${USER} ALL=(ALL)     NOPASSWD:ALL" > /etc/sudoers.d/${USER}; \
    chmod 440 /etc/sudoers.d/${USER}

# install java 
RUN yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel; \
    echo 'export JAVA_HOME=/usr/lib/jvm/java-1.8.0' > /etc/profile.d/java.sh; \
    echo 'export PATH=${PATH}:${JAVA_HOME}/bin' >> /etc/profile.d/java.sh

# install hadoop
ENV HADOOP_VERSION=2.8.3
RUN cd tmp; \
    wget http://ftp.jaist.ac.jp/pub/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz; \
    tar xfz hadoop-${HADOOP_VERSION}.tar.gz -C /opt; \
    rm -f hadoop-${HADOOP_VERSION}.tar.gz; \
    cd /opt; \
    ln -s hadoop-${HADOOP_VERSION} hadoop

# setup hadoop
ENV HADOOP_HOME=/opt/hadoop
RUN echo 'export HADOOP_HOME=/opt/hadoop' > /etc/profile.d/hadoop.sh; \
    echo 'export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop' >> /etc/profile.d/hadoop.sh; \
    echo 'export PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin' >> /etc/profile.d/hadoop.sh

# install spark
ENV SPARK_VERSION=2.2.1
RUN cd tmp; \
    wget http://ftp.jaist.ac.jp/pub/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz; \
    tar xfz spark-${SPARK_VERSION}-bin-hadoop2.7.tgz -C /opt; \
    rm -f spark-${SPARK_VERSION}-bin-hadoop2.7.tgz; \
    cd /opt; \
    ln -s spark-${SPARK_VERSION}-bin-hadoop2.7 spark

# setup spark
ENV SPARK_HOME=/opt/spark
RUN echo 'export SPARK_HOME=/opt/spark' > /etc/profile.d/spark.sh; \
    echo 'export PATH=${PATH}:${SPARK_HOME}/bin' >> /etc/profile.d/spark.sh

# install hive
ENV HIVE_VERSION=2.3.2
RUN cd tmp; \
    wget http://ftp.jaist.ac.jp/pub/apache/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz; \
    tar xfz apache-hive-${HIVE_VERSION}-bin.tar.gz -C /opt; \
    rm -f apache-hive-${HIVE_VERSION}-bin.tar.gz; \
    cd /opt; \
    ln -s apache-hive-${HIVE_VERSION}-bin hive

# setup hive
ENV HIVE_HOME=/opt/hive
RUN echo 'export HIVE_HOME=/opt/hive' > /etc/profile.d/hive.sh; \
    echo 'export PATH=${PATH}:${HIVE_HOME}/bin' >> /etc/profile.d/hive.sh

# install hbase
ENV HBASE_VERSION=1.4.1
RUN cd tmp; \
    wget http://ftp.jaist.ac.jp/pub/apache/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz; \
    tar xfz hbase-${HBASE_VERSION}-bin.tar.gz -C /opt; \
    rm -f hbase-${HBASE_VERSION}-bin.tar.gz; \
    cd /opt; \
    ln -s hbase-${HBASE_VERSION} hbase

# setup hbase
ENV HBASE_HOME=/opt/hbase
RUN echo 'export HBASE_HOME=/opt/hbase' > /etc/profile.d/hbase.sh; \
    echo 'export PATH=${PATH}:${HBASE_HOME}/bin' >> /etc/profile.d/hbase.sh

# change owner
RUN chown -R ${USER}:${USER} ${HADOOP_HOME}*; \
    chown -R ${USER}:${USER} ${SPARK_HOME}*; \
    chown -R ${USER}:${USER} ${HIVE_HOME}*; \
    chown -R ${USER}:${USER} ${HBASE_HOME}*

# remove multiple jar
RUN rm -f ${HIVE_HOME}/jdbc/hive-jdbc-2.3.2-standalone.jar; \
    rm -f ${HIVE_HOME}/lib/log4j-slf4j-impl-2.6.2.jar; \
    rm -f ${HBASE_HOME}/lib/slf4j-log4j12-1.7.10.jar

# start script
COPY start.sh /home/${USER}/
RUN chown ${USER}:${USER} /home/${USER}/*.sh; \
    chmod u+x /home/${USER}/*.sh

USER ${USER}
WORKDIR /home/${USER}
CMD sh /home/${USER}/start.sh
