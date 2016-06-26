# Dynamic Resource Scheduling of RESA

## Description
Resa-drs is the resource scheduling and management component of our [Resa-Project](http://www.resa-project.org/).

In a data stream management system (DSMS), users register continuous queries, and receive result updates as data arrive and expire. We focus on applications with real-time constraints, in which the user must receive each result update within a given period after the update occurs. To handle fast data, the DSMS is commonly placed on top of a cloud infrastructure. Because stream properties such as arrival rates can fluctuate unpredictably, cloud resources must be dynamically provisioned and scheduled accordingly to ensure real-time response. It is essential, for the existing systems or future developments, to possess the ability of scheduling resources dynamically according to the current workload, in order to avoid wasting resources, or failing in delivering correct results on time. 

Motivated by this, we propose DRS, a novel dynamic resource scheduler for cloud-based DSMSs. DRS overcomes three fundamental challenges: (a) how to model the relationship between the provisioned resources and query response time (b) where to best place resources; and (c) how to measure system load with minimal overhead. In particular, DRS includes an accurate performance model based on the theory of Jackson open queueing networks and is capable of handling arbitrary operator topologies, possibly with loops, splits and joins. 

## Developement
We have implemented the Resa-drs on top of the [Apahce Storm](http://storm.apache.org/) (ver. 1.0.1). The following figure shows the overview of the system architecture.
![Overview](/images/logo.png)
