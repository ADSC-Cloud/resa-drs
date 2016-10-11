# Dynamic Resource Scheduling of RESA

## Description
Resa-drs is the resource scheduling and management component of our [Resa-Project](http://www.resa-project.com/).

In a stream data analytics system, input data arrives continuously and triggers the processing and updating of analytics results. We focus on applications with real-time constraints, in which any data unit must be completely processed within a given time duration. To handle fast data, it is common to place the steam data analytics system on top of a cloud infrastructure. Because stream properties such as arrival rates can fluctuate unpredictably, cloud resources must be dynamically provisioned and scheduled accordingly to ensure real-time responses. It is essential, for existing systems or future developments, to possess the ability of scheduling resources dynamically according to the instantaneous workload, in order to avoid wasting resources or failing in delivering the correct analytics results on time. 

Motivated by this, we propose DRS, a novel dynamic resource scheduler for cloud-based stream data analytics systems. DRS overcomes three fundamental challenges: 
 1. how to model the relationship between the provisioned resources and query response time 
 2. where to best place resources to minimize tuple average complete latency
 3. how to implement resource scheduling on top of Apache Storm with minimal overhead

In particular, DRS includes an accurate performance model based on the theory of Jackson open queueing networks and is capable of handling arbitrary operator topologies, possibly with loops, splits and joins. 

## Developement
We have implemented the Resa-drs on top of the [Apahce Storm](http://storm.apache.org/) (ver. 1.0.1). The following figure shows the overview of the system architecture:

![Overview](/images/drsOverview.jpg)

More details of DRS modules can be refer to our academic paper "[DRS: Dynamic Resource Scheduling for Real-Time Analytics over Fast Streams](http://ieeexplore.ieee.org/xpl/articleDetails.jsp?arnumber=7164927)", which has been published in the proceedings of the 35th IEEE International Conference on Distributed Computing Systems (ICDCS 2015).

## How to use
### Download and deployment
The whole project is packaged into a .jar file: [resa-drs-0.1.0.jar](https://github.com/ADSC-Cloud/resa-drs/releases/download/v0.1.0/resa-drs-0.1.0.jar). You can also generate it through the source file:
 1. ```git clone https://github.com/fuzhengjia/resa-drs.git yourFolder```
 2. ```cd yourFolder```
 3. ```mvn clean package -DskipTests```
 4. now the resa-drs-0.1.0.jar should be under ```yourFolder/target/```

You shall put [resa-drs-0.1.0.jar](https://github.com/ADSC-Cloud/resa-drs/releases/download/v0.1.0/resa-drs-0.1.0.jar) and 
its dependencies (you can download them from [here](https://github.com/ADSC-Cloud/resa-drs/releases/download/v0.1.0/resa-drs-0.1.0-dependency.tar.bz2)):
 * commons-pool2-2.3.jar
 * hamcrest-core-1.1.jar
 * jackson-core-lgpl-1.9.13.jar
 * jackson-mapper-lgpl-1.9.13.jar
 * jedis-2.7.3.jar
 * json-simple-1.1.1.jar
 * junit-4.10.jar
 * snakeyaml-1.16.jar

into your ```Storm_Home/lib/```.

### Configuration and parameter settings (Storm cluster level)
You need to add the following into the storm.yaml file (by befault under the folder:  ```Storm_Home/conf/```) for all the nodes including the nimbus nodes and supervisor nodes:
```
nimbus.topology.validator: "resa.topology.ResaTopologyValidator"
topology.metrics.consumer.register:
  - class: "resa.topology.ResaContainer"
    parallelism.hint: 1
```

Note: after adding this setting, you need to **restart** the whole Storm cluster once, to activate resa-drs on storm!

### Configuration and parameter settings (Application level)
One advantage of Resa-drs is that it is transparent to the Storm-Topologies developed by users. All the Resa-drs funtionalities are automatically added and activated after user submit their *normal* topologies.

The only requirement is to add Resa-drs related configurations and parameter settings into the topology's configuration file when it is submitted by the user. You can refer to this [example.yaml](/conf/example.yaml) which contains all the relavent configurations. In this example file, there are three simulated topologies, whose definition is provided in the [src/test/java/TestTopology/simulated](/src/test/java/TestTopology/simulated).

## A running example of how Resa-drs works

This running example is basedn on "[SimExpServWCLoop.java](/src/test/java/TestTopology/simulated/SimExpServWCLoop.java)" topology in the test part of the Resa-drs source code)

Stage-1 After you submit the topology, if Resa-drs properly works, you can see it through Storm UI (click the "Show System Stats" button), and you will possibly see the following, where the initial configuration of this example topology (named "sim-loop-top") is:

Description of configuration | Value
 :--- | :---:
 Totoal number of workers | 3
 Number of executors of loop-Spout | 1
 Number of executors of loop-BoltA | 1
 Number of executors of loop-BoltB | 4
 Total number of executors used    | 6

![Drs-run](/images/drs-example-c1.jpg)
 
Stage-2 Accroding to the topology's configuration file [example.yaml](/conf/example.yaml), where we have "resa.opt.adjust.min.sec: 360", and "resa.opt.adjust.type: 0 (CurrentOpt)", we can see that the Topology's Rebalance Operation is triggered at around the 6th minute:
 
![Rebalance-triggered](/images/drs-example-c2.jpg)

Stage-3 When rebalance finishes, the new resource allocation (suggested by Resa-drs) is applied:
 
Description of configuration | Value
 :--- | :---:
 Number of workers | 3
 Number of executors of loop-Spout | 1
 Number of executors of loop-BoltA | 3
 Number of executors of loop-BoltB | 2
 Total number of executors used    | 6
 
Note, during the rebalance, the input data are accumulated at the queue (buffer) of data source (in our example, Redis is used). Therefore, when topology resumes after the rebalance, there is a impulse in the tuple average complete latency:

![After-Rebalance](/images/drs-example-c3.jpg)

Stage-4 After running for a while, no rebalance is triggered any more because the suggested resource allocation is always the same as the one which is already applied to the running topology. And we can see from the Storm UI, the accumlated tuple complete latency decrease a lot (note this value is an average over all the tuples completed after rebalance, and remember that right after the rebalance, this value is very large).

![After-a-while](/images/drs-example-c4.jpg)

In order to take a close look at the effectiveness of Resa-drs, we also plot the average tuple complete latency in each metric result window:

![Per-minute-plot](/images/drs-example-c6.jpg)

Note: you can click on "__metricsresa.topology.ResaContainer" and it will show more details of this system bolt, e.g. where it is hosted (node IP+port). Then, you can connect to the hosting node to check the log information, e.g. ```cat Storm_Home/logs/workers-artifacts/topology-name/port/worker.log | grep DefaultDecisionMaker```, for example: 

![log-informatoin](/images/drs-example-c5.jpg)
