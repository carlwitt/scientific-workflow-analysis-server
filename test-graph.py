"""
Tests the bokeh plotting facilities for the concurrent invocation count graph.
"""

__author__ = 'Carl Witt'
__email__ = 'wittcarl@deneb.uberspace.de'

from bokeh.plotting import figure, output_file, show

p = figure()
p.line([1,2,3,3.5,5],[1,1.5,2,2,1])
p.line([2,4,6,8,10],[3,2,1,2,3])
output_file("foo.html")
show(p)
