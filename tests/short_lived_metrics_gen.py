import numpy as np
import csv
import time
import random
import socket
import argparse
import string

parser=argparse.ArgumentParser()

parser.add_argument('--host', default='localhost')
parser.add_argument('--port', default=2003, type=int)
parser.add_argument('--datapoints', default=1440*3, type=int)
parser.add_argument('--paths-outer', default=100, type=int)
parser.add_argument('--paths-inner', default=100, type=int)
parser.add_argument('--total-metrics', default=100, type=int)
parser.add_argument('--total-pods', default=100, type=int)
parser.add_argument('--outfile', default="system_test.csv")
parser.add_argument('--stream', default=1, type=int)
parser.add_argument('--batches', default=1, type=int, help='total batches of metrics')

PATH_PATTERN = "performance.{}.conn-{}.{}.metric-{}"
args=parser.parse_args()
print(args)

def generate_sine_datapoints():
    n = args.datapoints
    limit_low = -10
    limit_high = 50000.48
    my_data = np.random.normal(0, 0.5, n) + np.abs(np.random.normal(0, 2, n) * np.sin(np.linspace(0, 3*np.pi, n)) ) + np.sin(np.linspace(0, 5*np.pi, n))**2 + np.sin(np.linspace(1, 6*np.pi, n))**2
    scaling = (limit_high - limit_low) / (max(my_data) - min(my_data))
    my_data = my_data * scaling
    my_data = my_data + (limit_low - min(my_data))
    return my_data

def generate_paths(resolution):
    paths = []
    pod_names = get_pod_names()
    for i in range(args.paths_outer):
        for pod in pod_names:
            for j in range(args.paths_inner):
                path = PATH_PATTERN.format(resolution, i, pod, j)
                paths.append(path)
    return paths

def write_metrics_to_file(writer, paths, start_time):
    for i in range(args.datapoints):
        print("writing datapoint %d of %d\n" % (i+1, args.datapoints))
        rows = []
        timestamp = start_time + 60 * i
        for idx, path in enumerate(paths):
            val = ts_data[idx % args.total_metrics][i]
            rows.append([timestamp, path, val])
        writer.writerows(rows)

def stream_metrics(server, paths, start_time):
    rows = []
    for i in range(args.datapoints):
        print("writing datapoint %d of %d for paths in batch \n" % (i+1, args.datapoints))
        timestamp = start_time - 60 * i
        for idx, path in enumerate(paths):
            val = ts_data[idx % args.total_metrics][i]
            str = "%s %d %d\n" % (path, val, timestamp)
            server.send(str.encode())

def get_pod_names():
    pod_names = []
    pod_file = open("pod_names.txt", "a+")
    for i in range(args.total_pods):
        pod_names.append(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8)))
    pod_file.write("\n".join(pod_names))
    pod_file.close()
    return pod_names

ts_data = []
for i in range(args.total_metrics):
    ts_data.append(generate_sine_datapoints())


current_time = int(time.time())
print("this is the first and current timestamp %d\n" % current_time)
print("also it is the max timestamp metric, meaning metrics are being written to older timestamps")

if args.stream != 1:
    file = open("system_test.csv", "w")
    writer = csv.writer(file)
    for i in range(10):
        paths = generate_paths("minutely")
        write_metrics_to_file(writer, paths, current_time - i * args.datapoints)
    file.close()
else:
    if args.stream == 1:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((args.host, args.port))
        for i in range(args.batches):
            print("executing batch %d of %d\n" % (i+1, args.batches))
            paths = generate_paths("minutely")
            stream_metrics(s, paths, current_time - i * args.datapoints)
