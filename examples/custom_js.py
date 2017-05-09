"""
	
"""

__author__ = 'Carl Witt'
__email__ = 'wittcarl@deneb.uberspace.de'


from bokeh.layouts import column
from bokeh.models import CustomJS, ColumnDataSource, Slider
from bokeh.plotting import Figure, output_file, show

output_file("callback.html")

x = [x*0.005 for x in range(0, 200)]
y = x

source = ColumnDataSource(data=dict(x=x, y=y))

plot = Figure(plot_width=400, plot_height=400)
plot.line('x', 'y', source=source, line_width=3, line_alpha=0.6)

callback = CustomJS(args=dict(source=source), code="""
        if( ! window.alreadyRegistered ){
        	var intervalID = setInterval(function(){source.trigger("change");}, 5000);
        	window.alreadyRegistered = true;
        }
    """)

slider = Slider(start=0.1, end=4, value=1, step=.1, title="power", callback=callback)

layout = column(slider, plot)

output_file("custom_js.html")
show(layout)