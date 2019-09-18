#!/usr/bin/env python3

import argparse
import random
import time

parser = argparse.ArgumentParser()

parser.add_argument('--paths-outer', default=10, type=int)
parser.add_argument('--paths-inner', default=10, type=int)
parser.add_argument('--host', default='localhost')
parser.add_argument('--port', default=8080, type=int)
parser.add_argument('--resolution', default="minutely")
parser.add_argument('--format', default="json")
parser.add_argument('--current_time', default=int(time.time()), type=int)
parser.add_argument('--glob_probability', default=80, type=int)
parser.add_argument('--total', default=200, help='total read requests to be generated', type=int)
parser.add_argument('--path-pattern', default="performance.{}.conn-{}.{}.metric-{}")
args = parser.parse_args()


def generate_paths(resolution):
    paths = []
    pod_names = get_generated_pod_names()
    for i in range(args.paths_outer):
        for pod in pod_names:
            for j in range(args.paths_inner):
                path = args.path_pattern.format(resolution, i, pod.rstrip(), j)
                paths.append(path)
    return paths


def get_generated_pod_names():
    pod_file = open("pod_names.txt", "r")
    pod_names = pod_file.readlines()
    return pod_names


paths = generate_paths(args.resolution)

f = open("generated_read_requests.txt", "w")
for i in range(args.total):
    random_path = random.choice(paths)
    if (random.randint(0, 100) < args.glob_probability):
        path_fragments = random_path.split('.')
        idx_to_replace = random.randint(len(path_fragments) - 2, len(path_fragments) - 1)
        path_fragments[idx_to_replace] = '*'
        random_path = ".".join(path_fragments)
    start_time = args.current_time - random.randint(2, 10) * 60 * 60
    read_request = f'GET http://{args.host}:{args.port}/render/?target={random_path}&format={args.format}&from={start_time}&until={args.current_time}\n'
    f.write(read_request)

f.close()
