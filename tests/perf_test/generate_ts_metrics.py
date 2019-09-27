import csv
import random
import time

import numpy as np

DATAPOINTS = 1440
PATHS_OUTER = 10
PATHS_INNER = 10
TOTAL_METRICS = 10
PATH_PATTERN = "performance.{}.conn-{}.metric-{}"


def generate_sine_datapoints():
    n = DATAPOINTS
    limit_low = -10
    limit_high = 50000.48
    my_data = np.random.normal(0, 0.5, n) + np.abs(np.random.normal(0, 2, n) * np.sin(np.linspace(0, 3 * np.pi, n))) + np.sin(np.linspace(0, 5 * np.pi, n)) ** 2 + np.sin(np.linspace(1, 6 * np.pi, n)) ** 2
    scaling = (limit_high - limit_low) / (max(my_data) - min(my_data))
    my_data = my_data * scaling
    my_data = my_data + (limit_low - min(my_data))
    return my_data


def generate_paths(resolution):
    paths = []
    for i in range(PATHS_OUTER):
        for j in range(PATHS_INNER):
            path = PATH_PATTERN.format(resolution, i, j)
            paths.append(path)
    return paths


def write_metric_to_file(metric, path):
    rows = []
    for idx, val in enumerate(metric):
        rows.append([current_time + 60 * idx, path, val])
    writer.writerows(rows)


ts_data = []
for i in range(TOTAL_METRICS):
    ts_data.append(generate_sine_datapoints())

file = open("system_test.csv", "w")
writer = csv.writer(file)
current_time = int(time.time())

paths = generate_paths("minutely")
for path in paths:
    write_metric_to_file(random.choice(ts_data), path)

file.close()
