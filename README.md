# Dynamic Resource Scheduling of RESA

## Description
Resa-drs is the resource scheduling and management component of our [Resa-Project](http://www.resa-project.org/).

In a data stream management system (DSMS), users register continuous queries, and receive result updates as data arrive and expire. We focus on applications with real-time constraints, in which the user must receive each result update within a given period after the update occurs. To handle fast data, the DSMS is commonly placed on top of a cloud infrastructure. Because stream properties such as arrival rates can fluctuate unpredictably, cloud resources must be dynamically provisioned and scheduled accordingly to ensure real-time response. It is essential, for the existing systems or future developments, to possess the ability of scheduling resources dynamically according to the current workload, in order to avoid wasting resources, or failing in delivering correct results on time. 

Motivated by this, we propose DRS, a novel dynamic resource scheduler for cloud-based DSMSs. DRS overcomes three fundamental challenges: (a) how to model the relationship between the provisioned resources and query response time (b) where to best place resources; and (c) how to measure system load with minimal overhead. In particular, DRS includes an accurate performance model based on the theory of Jackson open queueing networks and is capable of handling arbitrary operator topologies, possibly with loops, splits and joins. 

## Developement
We have implemented the Resa-drs on top of the [Apahce Storm](http://storm.apache.org/) (ver. 1.0.1). The following figure shows the overview of the system architecture:

![Overview](/images/drsOverview.jpg)

More details of DRS modules can be refer to our academic paper "[DRS: Dynamic Resource Scheduling for Real-Time Analytics over Fast Streams](http://ieeexplore.ieee.org/xpl/articleDetails.jsp?arnumber=7164927)", which has been published in the proceedings of the 35th IEEE International Conference on Distributed Computing Systems (ICDCS 2015).

## How to use
### Download and deployment
The whole project is packaged into a .jar file: [resa-drs-0.1.0.jar](http://www.resa-project.org/resa-drs-download/resa-drs-0.1.0.jar). You can also generate it through the source file:
 1. git clone https://github.com/fuzhengjia/resa-drs.git yourFolder
 2. cd yourFolder
 3. mvn clean package -DskipTests
 4. now the resa-drs-0.1.0.jar should be under yourFolder/target/

You shall put [resa-drs-0.1.0.jar](http://www.resa-project.org/resa-drs-download/resa-drs-0.1.0.jar) and 
its dependencies (you can download them from [here](http://www.resa-project.org/resa-drs-download/resa-drs-0.1.0-dependency.tar.bz2)):
 * commons-pool2-2.3.jar
 * hamcrest-core-1.1.jar
 * jackson-core-lgpl-1.9.13.jar
 * jackson-mapper-lgpl-1.9.13.jar
 * jedis-2.7.3.jar
 * json-simple-1.1.1.jar
 * junit-4.10.jar
 * snakeyaml-1.16.jar

into your Storm_Home/lib/ and remember to add their executable attribute: chmod +x resa-drs-0.1.0.jar


### Configuration and parameter settings

### Run applicatoin


