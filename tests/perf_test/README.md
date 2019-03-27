This doc describes how to use the scripts to set up performance testing for any backend for carbonapi. Below is a basic description of what each script does.

** short_lived_metrics_gen.py **

This script takes a bunch of parameters and generates metrics of the pattern *performance.{resolution}.conn-{paths_outer}.{total_pods}.metric-{paths_inner}* in batches. Based on how these parameters are configured, it can be used to generate many metrics in a batch, or relatively smaller number of metrics over large number of batches. The scenario of large number of batches simulates metrics getting generated with deployments in a kubernetes clkuster. The metrics generated can be streamed to a server, where the backend is deployed, or written to a file, from where they can be replayed over and over again to a backend, using the player. Every metric generated is a repeating sine wave.
Description of parameters:-
host: host to stream the metrics generated to
port: port to stream metrics to
datapoints: number of datapoints to be written for each metric
paths-outer: In the metric pattern, the paths outer part is replaced with numbers ranging from 0 to paths-outer - 1 
paths-inner: In the metric pattern, the paths inner part is replaced with numbers ranging from 0 to paths-inner - 1
total-metrics: This specifies the total number of sine curves generated, which are then randomnly assigned to different metric paths
total-pods: this specifies the total number of pod names generated per batch, which are then substituted in the total_pods specifier in path pattern, Every pod names generated is written to pod_names.txt
stream: Default is 1, when it streams the metrics to a server. Otherwise, it writes them to a file
outfile: If the metrics are being written to file, you can specify the filename to write these metrics to
batches: Total number of batches of metrics to generated. For each batch, metrics of the form path_pattern are generated. As the resolution is currently fixed to minutely, and paths outer and paths inner are numbers in the pattern, the pod names generated(equal to total pods in a batch), which is a randomly generated string, ensure that there is no collision across metrics in different batches. The timestamps in a metric go back in time, so for the first batch, it starts from (current time - datapoints * batches)

Further changes:- Currently, its not possible to distinguish from the outset which metrics are generated in which batch. The pattern can add a placeholder for batch number to solve this.



** gen_synthetic_reads.py **

This script generates the sythentic read requests for querying the backends. It writes to a file generated_read_requests.txt the requests, which are in the format where it can be used with slapper.
It tries to query the metrics which are generated in the previous script. 

Description of parameters:-
paths-outer: Used for generating the same pattern as before, paramter can be smaller as read requests will be a small sample
paths-inner: Used for generating the same pattern as before, paramter can be smaller as read requests will be a small sample
resolution: default set to minutely to generate same pattern of metrics for read requests
format: set to json by default, could be protobuf, if querying the backend directly.
current_time: pass this option as the same time stamp as the output from the generation script.
glob_probability: The read request generated need to have globs to query for multiple metrics. This parameter specifies, how many requests out of 100, will have globs in them.
total: total number of read requests generated.  
It reads the pod names generated in the generation script, and since these are random, it reads them from pod_names.txt

