# Dynamic Resource Scheduling of RESA

## Description
Resa-drs is the resource scheduling and management component of our [Resa-Project](http://www.resa-project.org/).

In a data stream management system (DSMS), users register continuous queries, and receive result updates as data arrive and expire. We focus on applications with real-time constraints, in which the user must receive each result update within a given period after the update occurs. To handle fast data, the DSMS is commonly placed on top of a cloud infrastructure. Because stream properties such as arrival rates can fluctuate unpredictably, cloud resources must be dynamically provisioned and scheduled accordingly to ensure real-time response. It is essential, for the existing systems or future developments, to possess the ability of scheduling resources dynamically according to the current workload, in order to avoid wasting resources, or failing in delivering correct results on time. 

Motivated by this, we propose DRS, a novel dynamic resource scheduler for cloud-based DSMSs. DRS overcomes three fundamental challenges: 
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
The whole project is packaged into a .jar file: [resa-drs-0.1.0.jar](http://www.resa-project.org/resa-drs-download/resa-drs-0.1.0.jar). You can also generate it through the source file:
 1. ```git clone https://github.com/fuzhengjia/resa-drs.git yourFolder```
 2. ```cd yourFolder```
 3. ```mvn clean package -DskipTests```
 4. now the resa-drs-0.1.0.jar should be under ```yourFolder/target/```

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

into your ```Storm_Home/lib/``` and remember to add their executable attribute: ```chmod +x resa-drs-0.1.0.jar```

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

The only requirement is to add Resa-drs related configurations and parameter settings into the topology's configuration file when it is submitted by the user. We list them in the following:
 * ```resa.metric.approved.names: - "complete-latency" - "execute" - "latency-stat" - "__sendqueue send-queue" - "__receive recv-queue" - "duration" - "arrival_rate_secs"   #The metrics defined and used by Resa-drs modules```
 * ```resa.optimize.alloc.class: "resa.optimize.MMKAllocCalculator"   #The optimal allocation calculator we designed and implemented based on Jackson Queueing network theory. In current version of Resa-drs, it is the only option. More options will be developed in future```
 * ```topology.builtin.metrics.bucket.size.secs: 60   #Storm built-in parameter, the period that the metrics are collected and reported by each task```
 *```resa.comp.sample.rate: 1.0  #the sample rate applied on measuring those appointed metric results```
 * ```resa.opt.win.history.size: 5    #The size of the history window. It decides how much historical metrics data needs to be buffered, e.g., when topology.builtin.metrics.bucket.size.secs: 60, only the metrics data reported in the previous 300 seconds will be maintained in the buffer.```  
 * ```resa.opt.win.history.ignore: -1  #In the beginning, how much reported metric data shall be ignored (during the system initilization on starting a topology, the metrics data are mostly unstable), -1 means the first group of reported  (at the 60th second) data needs to be ignored```
 * ```resa.optimize.interval.secs: 300   #The period that DRS will re-calculate the optimial allocation according to the metric data within the configured history window```
 * ```resa.scheduler.decision.class: "resa.drs.DefaultDecisionMaker"  #A simple decision maker we have implemented for making the decision when to trigger the Topology's rebalance operation. Note, the following parameters are valid only when "resa.drs.DefaultDecisionMaker" is configured.```
  * ```resa.opt.adjust.min.sec: 300  #The minimal expected interval that the "resa.drs.DefaultDecisionMaker" will trigger the Topology rebalance operation when it detects a better allocation suggested by drs allocation calculator.```
  * ```resa.opt.adjust.type: 0  #The type of suggested allocation to consider, 0: CurrentOpt(default) - where total number of executors used remain unchanged after rebalance; 1: MaxExecutorOpt - where the maximal available number (specified by the user through the next two parameters) of executors will be used after rebalance; 2: MinQoSOpt - where the minimal number of executors that can satisfy the user specified QoS target (maximum allowed expected tuple complete latency) will be used after rebalance (at current drs version, this type is not stable).```
  * ```resa.opt.smd.qos.ms: 1500    #user specified QoS target, i.e., the maximal allowed expected tuple complete latency in millisecond. It is effective only when resa.opt.adjust.type is set to 2.```
  * ```resa.topology.allowed.executor.num: 10   #user specified maximal available number of executors can be used. It is effective only when resa.opt.adjust.type is set to 1.```
  * ```resa.topology.max.executor.per.worker: 2   #user specified maximal number of executors can be assigned to each worker. Note the product (resa.topology.max.executor.per.worker * topology.NumberOfWorkers) is an upper bound of total available number of executors can be used and it is effective for all the three types!``` 
 * ```#resa.scheduler.decision.class: "resa.drs.EmptyDecisionMaker"  #This is an alternative decision maker implementation, with automatically triggering the Topology's rebalance operation disabled. Note, Resa-drs is still (passively) working, to generate measurement results, calculate and suggest optimal allocations. However, users (if they intend to) have to trigger the Topology's rebalance operation manually (either by commond line, i.e. "Storm_Home/bin/storm rebalance ... " or through Storm UI.```



