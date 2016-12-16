#!/usr/bin/env python

import itertools
import json
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import os
import requests
import signal
import time

from matplotlib.font_manager import FontProperties
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Misc config
executor = 'mesos'
health_checker = 'marathon'
num_nodes = 1
protocol = 'TCP'
base_dir = '{}-{}-{}-{}'.format(executor, health_checker, protocol, num_nodes)
step = 10

# HTTP config
base = 'https://35.165.141.114/marathon'
headers = {
        'Authorization': 'token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJleHAiOjE0ODIzMzUwNjAsInVpZCI6ImJvb3RzdHJhcHVzZXIifQ.MG4kqnwS2orDx0XkwzvvZgUSWI0EEp8CswCNkaDTDl_XKFTO6G_lsazYwpM17aHK1UFT3e7y3HOLQgfFevTTrjckhM0mm2C-jCew8rDJ_cOJ_Hkv19Xgnt1gS5Y2_LKoc-ngugk-2_5HzA2Qf5UrUwRuiMKjAO2K-Kt_O-isGuNqsIOqX2ZsTYuMu5zyB19iQWMpDJFlbSLjUf5H7ORUDD435QgYUVA7G_qgkC78be4SEdjGD3BD9K8K6Dxa2HKlj6qZRKNsvpZyG3nhGIZI0dwAzaSNmGqyNMR-vVi0LLuxBaBlnKiu1Iz_227zf0OtC6iKQPNnLaILVt1SLY79Kg',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
        }
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Plot config
title = '{} {} check - {} Executor - {} Agent(s)'.format(health_checker.title(), protocol,
                                                         executor.title(), num_nodes)
states = [
        'TASK_HEALTHY',
        'TASK_UNHEALTHY',
        'TASK_KILLING',
        'TASK_RUNNING',
        'TASK_STAGING',
        'UNSCHEDULED'
        ]

data = {
        'TASK_HEALTHY': {'count': [], 'color': 'darksage'},
        'TASK_RUNNING': {'count': [], 'color': 'steelblue'},
        'TASK_STAGING': {'count': [], 'color': '0.55'},
        'TASK_UNHEALTHY': {'count': [], 'color': 'indianred'},
        'TASK_KILLING': {'count': [], 'color': 'purple'},
        'UNSCHEDULED': {'count': [], 'color': '0.85'}
        }

# Ugly global state
max_count = 0
start_time = time.time()
shutting_down = False


def scale_to(instances, time_delta, force=False):
    print '==================================='
    print 'Time: {} - Scaling to {}'.format(time_delta, instances)
    print '==================================='
    print

    url = '{}/v2/apps/hc'.format(base)

    payload = {'instances': instances}

    params = {'force': 'true' if force else 'false'}

    r = requests.put(url, headers=headers, params=params, json=payload, verify=False)

    r.raise_for_status()


def fetch_app():
    url = '{}/v2/apps/hc'.format(base)
    params = {'embed': ['apps.deployments', 'apps.tasks']}

    r = requests.get(url, headers=headers, params=params, verify=False)

    r.raise_for_status()

    return r.json()['app']


def task_status(task):
    if task['state'] == 'TASK_RUNNING':
        # Task running
        if 'healthCheckResults' in task:
            if task['healthCheckResults'][0]['alive']:
                return 'TASK_HEALTHY'
            else:
                return 'TASK_UNHEALTHY'

    return task['state']


def print_task_summary(app, time_delta):
    tasks = app['tasks']
    instances = app['instances']

    print 'Time: {}\tTasks: {}/{}'.format(time_delta, len(tasks), instances)
    print '==================================='

    tasks_by_state = itertools.groupby(sorted(tasks, key=task_status), key=task_status)

    for state, taskIter in tasks_by_state:
        tasks = list(taskIter)
        print '{}\t{}/{}'.format(state, len(tasks), instances)

    print '==================================='
    print


def process_results(app):
    tasks = app['tasks']
    goal = app['instances']

    iterator = itertools.groupby(sorted(tasks, key=task_status), key=task_status)
    tasks_by_state = dict((k, list(g)) for k, g in iterator)
    acc = 0
    for state in states[:-1]:
        if state in tasks_by_state.keys():
            count = len(tasks_by_state[state])
            acc = acc + count
            data[state]['count'].append(count)
        else:
            data[state]['count'].append(0)

    data['UNSCHEDULED']['count'].append(goal - acc)


def plot(time_delta):
    fig = plt.figure(dpi=100, figsize=(10.24, 7.68))
    ax = plt.subplot(111)

    num_requests = len(data[states[0]]['count'])
    ind = np.arange(0, num_requests) # the x locations

    bottom = np.zeros(num_requests)
    for state in states:
        counts = np.array(data[state]['count'])
        color = data[state]['color']
        ax.bar(ind, counts, 0.75, color=color, linewidth=0, bottom=bottom)

        bottom = bottom + counts

    plt.title(title)

    plt.xlim(xmin=0, xmax=num_requests + 1)
    plt.xlabel('Request #')

    plt.ylabel('Number of tasks')

    # Make the legend font small
    font = FontProperties()
    font.set_size('small')

    ax.legend([state.replace('TASK_', '') for state in states],
            loc='upper left', prop=font)

    plt.savefig('{}/{}.png'.format(base_dir, time_delta))
    plt.close(fig)


def dump_app(app, time_delta):
    f = open('{}/{}.json'.format(base_dir, time_delta), 'w')
    f.write(json.dumps(app, indent=2))
    f.close()


"""Main testing loop.

It will:

    1. Scale the app down to 0 instances.
    2. Increase the instances count by 10 tasks and wait for the deployment to
       complete.
    3. Go back to the previous step.
"""


def main_loop():
    global start_time

    scale_down()

    start_time = time.time()
    target = 0
    while True:
        time.sleep(1)
        time_delta = int(time.time() - start_time)

        try:
            app = fetch_app()

            if not app['deployments']:
                target = target + step
                scale_to(target, time_delta)

            print_task_summary(app, time_delta)
            if not shutting_down:
                process_results(app)
                plot(time_delta)
                dump_app(app, time_delta)
        except Exception as err:
            print err


"""Scale down to 0 instances and wait for the deployment to complete.

The deployment is forced, overriding any other pending deployments.
"""


def scale_down():
    time_delta = int(time.time() - start_time)

    deploymentId = scale_to(0, time_delta, True)
    time.sleep(1)

    app = fetch_app()
    while app['deployments']:
        time_delta = int(time.time() - start_time)
        print_task_summary(app, time_delta)

        time.sleep(1)
        app = fetch_app()


def handler(signum, frame):
    global shutting_down

    # let the next ctrl + c kill the test.
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    shutting_down = True
    scale_down()
    exit(0)


signal.signal(signal.SIGINT, handler)

if not os.path.exists(base_dir):
    os.makedirs(base_dir)

main_loop()
