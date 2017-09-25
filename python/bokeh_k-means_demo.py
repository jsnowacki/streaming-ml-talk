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

# Algorithm based on:
# Web-Scale K-Means Clustering, D. Sculley

import numpy as np
from bokeh.layouts import layout
from bokeh.models import ColumnDataSource
from bokeh.models.widgets import Select
from bokeh.palettes import Colorblind3 as pallet
from bokeh.plotting import curdoc, figure

# this must only be modified from a Bokeh session callback
points = ColumnDataSource(data=dict(x=[], y=[], color=[]))
centroids = ColumnDataSource(data=dict(x=[], y=[], color=[]))
centers = ColumnDataSource(data=dict(x=[], y=[], color=[]))

center_drift = Select(title='Center drift:', value='False', options=['False', 'True'])
adaptive = Select(title='Adaptive:', value='True', options=['False', 'True'])


# This is important! Save curdoc() to make sure all threads
# see then same document.
doc = curdoc()

t = 0
n = 0
x_c = np.array([-1, 0, 1], dtype=np.float)
y_c = np.array([-1, 0, -1], dtype=np.float)


def cluster_simulations():
    global t, n, x_c, y_c

    n += 1
    q = np.array([t + -3/4*np.pi, t, t + 3/4*np.pi])
    x = 2*np.sin(q)
    y = 2*np.cos(q)
    x_r = x + np.random.randn(3)/2
    y_r = y + np.random.randn(3)/2
    color = slice(0, 3)

    # Get the closest centriods
    ctr = np.argmin((np.matrix(x_r).T * np.ones((1, 3)) - x_c)**2 +
                    (np.matrix(y_r).T * np.ones((1, 3)) - y_c)**2, axis=0) \
        .tolist()[0]

    if adaptive.value == "True":
        d = 0.2
    else:
        d = 1/n

    # Update centroids
    for i, k in enumerate(ctr):
        x_c[k] = (1 - d)*x_c[k] + d*x_r[i]
        y_c[k] = (1 - d)*y_c[k] + d*y_r[i]

    # Update data sources
    centers.stream(dict(x=x, y=y, color=pallet[color]), rollover=3)
    points.stream(dict(x=x_r, y=y_r, color=pallet[color]), rollover=90)

    colors = np.array([pallet[i] for i in ctr])
    centroids.stream(dict(x=x_c, y=y_c, color=colors), rollover=3)

    if center_drift.value == "True":
        t += 0.02


doc.add_periodic_callback(cluster_simulations, 200)

p = figure(x_range=[-4, 4], y_range=[-4, 4])
p.circle(x='x', y='y', color='color', source=points)
p.cross(x='x', y='y', color='color', size=14, source=centers)
p.x(x='x', y='y', color='color', size=14, source=centroids)

p.xaxis.axis_label = "X"
p.yaxis.axis_label = "Y"

l = layout([center_drift, adaptive], [p])

doc.add_root(l)

