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
from bokeh.models.widgets import Select, Slider
from bokeh.palettes import Colorblind3 as palette
from bokeh.plotting import curdoc, figure

# this must only be modified from a Bokeh session callback
points = ColumnDataSource(data=dict(x=[], y=[], color=[]))
centroids = ColumnDataSource(data=dict(x=[], y=[], color=[]))
centers = ColumnDataSource(data=dict(x=[], y=[], color=[]))

center_drift = Select(title='Center drift:', value='False', options=['False', 'True'])
alpha = Slider(title='Alpha', value=0.2, start=0, end=1, step=0.1)


# This is important! Save curdoc() to make sure all threads
# see then same document.
doc = curdoc()

k = 3  # number of clusters
n_b = 4  # batch size (will be times k)
t = 0  # time

n = np.zeros(k)  # initial n per cluster
theta = np.linspace(0, 2*np.pi, k, endpoint=False)  # center points spread on a circle
r = 2.5  # radius of circle

# Initial centres
x_c = r*np.sin(theta)
y_c = r*np.cos(theta)

# Get a color according to a palette
get_color = np.vectorize(lambda c: palette[c])


def cluster_simulations():
    global t, n, x_c, y_c, k, n_b
    a = alpha.value

    q = np.array(np.tile(theta + t, n_b))
    x = r*np.sin(q)
    y = r*np.cos(q)
    x_r = x + np.random.randn(k*n_b)/2
    y_r = y + np.random.randn(k*n_b)/2

    # Get the closest centriods
    ctr = np.argmin((np.matrix(x_r).T * np.ones((1, k)) - x_c).A**2 +
                    (np.matrix(y_r).T * np.ones((1, k)) - y_c).A**2, axis=1)

    # Update centroids
    x_t = np.zeros(3)
    y_t = np.zeros(3)
    m_t = np.zeros(3)
    for i in range(k):
        m_t[i] = x_r[ctr == i].size
        x_t[i] = x_r[ctr == i].sum()
        y_t[i] = y_r[ctr == i].sum()
    x_c = ((x_c * n * a) + x_t) / (n*a + m_t)
    y_c = ((y_c * n * a) + y_t) / (n*a + m_t)
    n = n*a + m_t

    # Update data sources
    color = get_color(ctr)
    centers.stream(dict(x=x, y=y, color=color), rollover=3)
    points.stream(dict(x=x_r, y=y_r, color=color), rollover=90)
    centroids.stream(dict(x=x_c, y=y_c, color=get_color(range(3))), rollover=3)

    if center_drift.value == "True":
        t += 0.02


doc.add_periodic_callback(cluster_simulations, 200)

p = figure(x_range=[-4, 4], y_range=[-4, 4])
p.circle(x='x', y='y', color='color', source=points)
p.cross(x='x', y='y', color='color', size=14, source=centers)
p.x(x='x', y='y', color='color', size=14, source=centroids)

p.xaxis.axis_label = "X"
p.yaxis.axis_label = "Y"

l = layout([center_drift, alpha], [p])

doc.add_root(l)

