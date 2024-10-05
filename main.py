from fastapi import FastAPI, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials = True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

@app.post('/pipelines/parse')
async def parse_pipeline(request: Request):
    form_data = await request.form()
    pipeline = form_data.get('pipeline', '{}')
    pipeline = json.loads(pipeline)

    nodes = pipeline.get('nodes', [])
    edges = pipeline.get('edges', [])

    num_nodes = len(nodes)
    num_edges = len(edges)

    is_dag = check_dag(nodes, edges)

    return {
        'status': 'parsed',
        'num_nodes': num_nodes,
        'num_edges': num_edges,
        'is_dag': is_dag
    }

def check_dag(nodes: List[dict], edges: List[dict]) -> bool:
    from collections import defaultdict, deque

    graph = defaultdict(list)
    in_degree = defaultdict(int)

    for edge in edges:
        source = edge['source']
        target = edge['target']
        graph[source].append(target)
        in_degree[target] += 1
        if source not in in_degree:
            in_degree[source] = 0

    zero_in_degree = deque([node['id'] for node in nodes if in_degree[node['id']] == 0])
    visited_count = 0

    while zero_in_degree:
        node = zero_in_degree.popleft()
        visited_count += 1

    for neighbor in graph[node]:
        in_degree[neighbor] -= 1
        if in_degree[neighbor] == 0:
            zero_in_degree.append(neighbor)

    return visited_count == len(nodes)

