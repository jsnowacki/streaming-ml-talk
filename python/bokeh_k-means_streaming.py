#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from collections import deque
from threading import Thread

from bokeh.models import ColumnDataSource
from bokeh.palettes import Colorblind3 as pallet
from bokeh.plotting import curdoc, figure

# this must only be modified from a Bokeh session callback
from socket_server import ThreadedServer

points = ColumnDataSource(data=dict(x=[], y=[], color=[]))
centers = ColumnDataSource(data=dict(x=3*[None], y=3*[None], color=pallet))

# This is important! Save curdoc() to make sure all threads
# see then same document.
doc = curdoc()

data = deque()


def update():
    # Safer than while
    x_p, y_p = list(), list()
    x_c, y_c = centers.data['x'], centers.data['y']
    color = list()
    for _ in range(len(data)):
        row = json.loads(data.popleft())
        x_p.append(row['point']['x'])
        y_p.append(row['point']['y'])
        i = row['label']
        color.append(pallet[i])
        x_c[i] = row['center']['x']
        y_c[i] = row['center']['y']

    points.stream(dict(x=x_p, y=y_p, color=color), rollover=90)
    centers.stream(dict(x=x_c, y=y_c, color=pallet), rollover=3)


p = figure(x_range=[-4, 4], y_range=[-4, 4])
p.circle(x='x', y='y', color='color', source=points)
p.x(x='x', y='y', color='color', size=14, source=centers)

p.xaxis.axis_label = "X"
p.yaxis.axis_label = "Y"

doc.add_root(p)
doc.add_periodic_callback(update, 200)


def run_socket_server():
    socket_server = ThreadedServer(host="localhost", port=9911, deque=data)
    socket_server.listen()


thread = Thread(target=run_socket_server)
thread.start()
