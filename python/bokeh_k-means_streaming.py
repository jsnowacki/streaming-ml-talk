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


from functools import partial
from threading import Thread
import numpy as np
import time

from bokeh.models import ColumnDataSource
from bokeh.plotting import curdoc, figure
from bokeh.palettes import Colorblind3 as pallet
from bokeh.layouts import widgetbox, layout
from bokeh.models.widgets import Button, RadioButtonGroup, Select, Slider


from tornado import gen

# this must only be modified from a Bokeh session callback
points = ColumnDataSource(data=dict(x=[], y=[], color=[]))
centroids = ColumnDataSource(data=dict(x=[], y=[], color=[]))
centers = ColumnDataSource(data=dict(x=[], y=[], color=[]))

center_drift = Select(title='Center drift:', value='False', options=['False', 'True'])
adaptive = Select(title='Adaptive:', value='True', options=['False', 'True'])


# This is important! Save curdoc() to make sure all threads
# see then same document.
doc = curdoc()


@gen.coroutine
def update_points(x, y, color):
    points.stream(dict(x=x, y=y, color=pallet[color]), rollover=90)


@gen.coroutine
def update_centers(x, y, color):
    centers.stream(dict(x=x, y=y, color=pallet[color]), rollover=3)


@gen.coroutine
def update_centroids(x, y, color):
    colors = np.array([pallet[i] for i in color])
    centroids.stream(dict(x=x, y=y, color=colors), rollover=3)


def cluster_simulations():
    t = 0
    n = 0

    x_c = np.array([-1, 0, 1], dtype=np.float)
    y_c = np.array([-1, 0, -1], dtype=np.float)
    while True:
        # do some blocking computation
        time.sleep(0.2)
        n += 1
        q = np.array([t + -3/4*np.pi, t, t + 3/4*np.pi])
        x = 2*np.sin(q)
        y = 2*np.cos(q)
        x_r = x + np.random.randn(3)/2
        y_r = y + np.random.randn(3)/2
        color = slice(0, 3)

        # Get the closest centriods
        i_c = np.argmin((np.matrix(x_r).T * np.ones((1, 3)) - x_c)**2 +
                        (np.matrix(y_r).T * np.ones((1, 3)) - y_c)**2, axis=0) \
            .tolist()[0]

        if adaptive.value == "True":
            d = 0.2
        else:
            d = 1/n

        # Update centroids
        for i, k in enumerate(i_c):
            x_c[k] = (1 - d)*x_c[k] + d*x_r[i]
            y_c[k] = (1 - d)*y_c[k] + d*y_r[i]


        # Sent out the data to
        doc.add_next_tick_callback(partial(update_points, x=x_r, y=y_r, color=color))
        doc.add_next_tick_callback(partial(update_centers, x=x, y=y, color=color))
        doc.add_next_tick_callback(partial(update_centroids, x=x_c, y=y_c, color=i_c))

        if center_drift.value == "True":
            t += 0.02


p = figure(x_range=[-4, 4], y_range=[-4, 4])
p.circle(x='x', y='y', color='color', source=points)
p.cross(x='x', y='y', color='color', size=14, source=centers)
p.x(x='x', y='y', color='color', size=14, source=centroids)

l = layout([widgetbox(center_drift, adaptive)], [p])

doc.add_root(l)

thread = Thread(target=cluster_simulations)
thread.start()
