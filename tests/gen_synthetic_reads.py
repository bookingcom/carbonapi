import argparse
import time
import random

parser=argparse.ArgumentParser()


parser.add_argument('--datapoints', default=1440)
parser.add_argument('--outer', default=10)
parser.add_argument('--inner', default=10)
parser.add_argument('--host', default='localhost')
parser.add_argument('--port', default=2003)
parser.add_argument('--resolution', default="minutely")
parser.add_argument('--format', default="json")
parser.add_argument('--current_time', default=int(time.time()))
parser.add_argument('--glob_probability', default=20)
parser.add_argument('--total', default=200, help='total read requests to be generated')
parser.add_argument('--pattern', default="performance.{}.conn-{}.metric-{}")

args=parser.parse_args()

def generate_paths(resolution):
    paths = []
    path_pattern = args.pattern
    for i in  range(args.outer):
        for j in range(args.inner):
            path = path_pattern.format(resolution, i, j)
            paths.append(path)
    return paths

paths = generate_paths(args.resolution)

f = open("generated_read_requests.txt", "w")
for i in range(args.total):
    random_path = random.choice(paths)
    if (random.randint(0,100) < args.glob_probability):
        path_fragments = random_path.split('.')
        idx_to_replace = random.randint(len(path_fragments)-2, len(path_fragments)-1)
        path_fragments[idx_to_replace] = '*'
        random_path = ".".join(path_fragments)
    start_time = args.current_time - random.randint(2,10)*60*60
    read_request = f'GET {args.host}:{args.port}/render/?target={random_path}&format={args.format}&from={start_time}&until={args.current_time}\n'
    f.write(read_request)
