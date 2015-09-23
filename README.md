Apache Spark job server
-----------------------------

The goal of this project is to initialize Apache `SparkContext` locally for every unique Spark app/job.
Jobs are executed in the context of so called `container` (own class path loader). By doing this it is possible to execute
jobs in already instantiated `SparkContext`, so much faster. Changed job parameters do not need new `SparkContext` instance.


# Prerequisites

## [Hadoop/HDFS](http://hadoop.apache.org/docs/current/)
The solution works with **Hadoop** version *2.7.1*. Hadoop is used mainly as a distributed file system - HDFS.
The installation instructions for pseudo-distributed mode: [Hadoop Installation](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation).
Great explanation of the OSX installation procesess: [OSX Installation](http://joeyoung.io/installing-hadoop-and-yarn-on-os-x-trials-troubleshooting-and-work-arounds/).

Once Haoop is installed and configured the following instructions start the name and data nodes.

    $ cd /usr/local/hadoop
    $ ./sbin/start-dfs.sh
    
## [Apache Spark](http://spark.apache.org) standalone

Apache Spark is a cluster computing system. We tend to use [standalone](http://spark.apache.org/docs/latest/spark-standalone.html) installation.

Installation hints: go to the [downloads page](http://spark.apache.org/downloads.html) and choose:

* Spark release - 1.5.0
* Package type - pre-build with user-provided Hadoop (we assume that hadoop is already installed).

Unpack the distribution and open `conf/spark-env.sh` file. 
Add the following settings.

    # MANDATORY: the explicit path to 'hadoop' binary
    export SPARK_DIST_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)

    # OPTIONAL - SPARK_MASTER_IP, to bind the master to a different IP address or hostname
    SPARK_MASTER_IP=localhost

### Apache Spark recovery settings

Apache Spark has built-in support for slave/worker nodes failures. But the master node is single point of failure.
The description of high availability configuration with ZooKeeper can be find in [documentation](http://spark.apache.org/docs/latest/spark-standalone.html#high-availability).

If we want to keep single node configuration bewteen master node restarts we can setup the following property in `conf/spark-env.sh`

    # set config properties for all daemons (e.g. "-Dx=y")
    SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=FILESYSTEM -Dspark.deploy.recoveryDirectory=recovery"

## Import example access log data into the HDFS

We assume that the current user is used as a hadoop user too. So create folder structure in hdfs directly. 

    $ echo $USER
    $ hdfs dfs -mkdir -p /user/$USER/input

Import example access log data into the hdfs file system.
 
    $ cd spark-module-topn/src/test/resources/input
    $ hdfs dfs -copyFromLocal access*.log /user/$USER/input


# Settings

Before the launch there is a need to set up some of the settings.

## ratpack-app/src/ratpack/application.properties

**spark.libsDir** - a path to folder with all Apache Spark dependencies. It is the results of 
`./gradlew :spark-module-deps:installDist` command.

    spark.libsDir=/Users/zedar/dev/hadoopdev/ratpack-hadoop-spark/spark-module-deps/build/install/lib/

**spark.homeDir** - a path to folder where apache spark is installed. This is not used in server mode.

    spark.homeDir=/Users/zedar/dev/hadoopdev/spark-1.5.0-bin-without-hadoop

**spark.user** - a user in context of which we access spark server as well as HDFS file system

    spark.user=zedar

**spark.master** - address of the Spark server either local, or standalone.

    spark.master=spark://localhost:7077

**spark.maxCoresPerTask** - maximum number of cores to be used by one Spark job. Default value is `2`.

    spark.maxCoresPerTask=2

**spark.fileSystemHost** - HDFS file system server

    spark.fileSystemHost=localhost

**spark.fileSystemPort** - HDFS file system port

    spark.fileSystemPort=54310

## ratpack-app/src/ratpack/sparkjobs.properties

**job.topNJarsDir** - a path to folder with jars needed by TopN Spark job.

    job.topNJarsDir=/Users/zedar/dev/hadoopdev/ratpack-spark-job-server/spark-module-topn/build/libs/

# Spark Job Container

Apache Spark jobs are executed in own containers, that in one-to-one relationship with `SparkContext`.
Initialization of `SparkContext` is expensive and has to be one threaded. So all requests to the same container have to
wait for `SparkContext` initialization.

A container loads all dependecies throughout own class loader. Then conflicts between Ratpack and Spark `jars` are eliminated.

# Build and run the job server

* Module: **spark-module-deps**: all dependencies necessary to execute Spark jobs. They are not included directly in 
Ratpack application because of so many conflicts. They are loaded in seperate class loader used by so called *containers*
holding *SparkContext*.
* Module: **spark-module-topn**: implementation of calculating the most active N users based on the specific access log.
* Module: **ratpack-app**: the server with endpoints to execute Spark jobs

Every Spark job is executed in seperate `Container`. There is a hirarchy of class loaders. The common (root) class loader 
with all Apache Spark dependencies. Every `Container` has its own class loader with common (root) set as parent.

Starting the server with building all dependencies:

    $ ./gradlew run
    
Starting and executing the topN job:

    $ curl -XPOST -H "Content-Type: application/json" -d '{"limit": 11, "timeInterval": {"dateFrom": "2015-08-21", "dateTo": "2015-08-22"}}' http://localhost:5050/v1/spark/top

Note, that the first execution of the job could take more time and, it is very important, blocks the other executions of the same job.
This is, because initialization of Spark Job on the Spark server, deployment of the application in the claster takes some time.
The next job executions should be much faster.

