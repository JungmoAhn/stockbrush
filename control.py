# -*- coding:utf-8 -*
from os.path import dirname, join

import numpy as np
import pandas as pd
import pandas.io.sql as psql
import sqlite3 as sql
from bokeh.plotting import Figure
from bokeh.plotting import figure, output_file, show
from bokeh.models import ColumnDataSource, HoverTool, HBox, VBoxForm, OpenURL, TapTool
from bokeh.models.widgets import Slider, Select, TextInput, RadioButtonGroup, Toggle, Panel, Tabs
from bokeh.io import curdoc
import sys
sys.path.append('/Users/jman/work/stockbrush')
from stockbrush import StockBrush
import copy

sb = StockBrush(nosave_bypass_latest=1)
sb.get_info()

max_weeks = 0

for key, exchange in sb.exchanges.items() :
    color = ['red' if symbol['overwall'] == True else 'black' for idx, symbol in exchange.iterrows()]
    exchange['color'] = color
    color = ['blue' if symbol['turnaround'] == True else symbol['color'] for idx, symbol in exchange.iterrows()]
    sb.exchanges[key]['color'] = color
    sb.exchanges[key]['alpha'] = 0.5
    weeks = max(sb.exchanges[key]['weeks'])
    max_weeks = max(max_weeks, weeks)

''''
with open(join(dirname(__file__), "razzies-clean.csv")) as f:
    razzies = f.read().splitlines()
kosdaq_view.loc[kosdaq_view.imdbID.isin(razzies), "color"] = "purple"
kosdaq_view.loc[kosdaq_view.imdbID.isin(razzies), "alpha"] = 0.9
'''
axis_map = {
    "QuoteLast": "QuoteLast",
    "shares": "shares",
    }

#max_weeks = max(kosdaq_view['weeks'])
    
# Create Input controls
#year_markdown_percent = Slider(title="year_markdown_percent", value=0, start=0, end=100, step=10)
week_period = Slider(title="week_period", value=52, start=1, end=max_weeks, step=1)
week_markdown_percent = Slider(title="week_markdown_percent", value=0, start=0, end=100, step=1)
max_week_close_index = Slider(title="period_max_week_close_index", value=0, start=0, end=max_weeks, step=1)
strait_markup_weeks = Slider(title="strait_markup_weeks", value=0, start=0, end=10, step=1)
strait_markdown_weeks = Slider(title="strait_markdown_weeks", value=0, start=0, end=10, step=1)
max_inst_vol = Slider(title="max_inst_vol", value=0, start=0, end=52, step=1)

exchange_option = Select(title='exchanges', value='kosdaq', options=['kosdaq', 'krx'])

check_ticket_options = RadioButtonGroup(
    labels=["max", "sixweeks_markup"], active=0)

#year_markdown_percent = TextInput(title="year_markdown_percent")
#week_markdown_percent = TextInput(title="week_markdown_percent")
#week_period = TextInput(title="week_period")
#max_week_close_index = TextInput(title="max_week_close_index")

#genre = Select(title="Genre", value="All",
#               options=open(join(dirname(__file__), 'genres.txt')).read().split())
#director = TextInput(title="Director name contains")
x_axis = Select(title="X Axis", options=sorted(axis_map.keys()), value="QuoteLast")
y_axis = Select(title="Y Axis", options=sorted(axis_map.keys()), value="shares")

# Create Column Data Source that will be used by the plot
source = ColumnDataSource(data=dict(x=[], y=[], color=[], alpha=[],title=[], code=[]))

hover = HoverTool(tooltips=[
    ("Title","@title"),
])

tap = TapTool()

print(type(tap))

p = Figure(plot_height=600, plot_width=800, title="", toolbar_location=None, tools=[hover, tap])
p.circle(x="x", y="y", source=source, size=10, color="color", line_color=None, fill_alpha="alpha")
tab1 = Panel(child=p, title='markdown')

#p2 = Figure(plot_height=600, plot_width=800, title="", toolbar_location=None, tools=[hover, tap])
#p2.circle(x="x", y="y", source=source, size=7, color="color", line_color=None, fill_alpha="alpha")
#tab2 = Panel(child=p2, title='max')

#tabs = Tabs(tabs=[tab1, tab2])

#url = "https://www.google.com/finance?q=KOSDAQ%3A@code&ei=UVfqVtDqNYua0ASSvI7QDg"
url = 'http://finance.naver.com/item/main.nhn?code=@code'
taptool = p.select(type=TapTool)
taptool.callback = OpenURL(url=url)

def select_tickets():
    for key, exchange in sb.exchanges.items() :
        if exchange_option.value == key :
            selected = exchange[(exchange.overwall == True) | (exchange.turnaround == True)]
            if strait_markup_weeks.value != 0 :
                selected = exchange[
                    (exchange.strait_markup_weeks == strait_markup_weeks.value)]

            if strait_markdown_weeks.value != 0 :
                selected = exchange[
                    (exchange.strait_markdown_weeks == strait_markdown_weeks.value)]

            if max_inst_vol.value != 0 :
                exchange['max_inst_deal_volume'] = 0
                sb.get_inst_info(skip=max_inst_vol.value)
                selected = exchange[
                    (exchange.max_inst_deal_volume <= exchange.last_inst_deal_volume) & (exchange.max_inst_deal_volume > 0)]

    return selected                
    '''
        for idx, symbol in kosdaq_view.iterrows() :
        weeks = int(symbol['weeks'])
        index = weeks-week_period.value if weeks > week_period.value else 0
        symbol['week_close_period'] = sticket.kosdaq['week_close'][idx][index:]
        del kosdaq_view['max_week_close'][idx]        
        kosdaq_view['max_week_close'][idx] = max(symbol['week_close_period'])
        del kosdaq_view['max_week_close_index'][idx]        
        kosdaq_view['max_week_close_index'][idx] = symbol['week_close_period'].index(max(symbol['week_close_period']))
        if week_markdown_percent.value != 0 :
        selected = kosdaq_view[
            #price:max_week_close=x:100
            (kosdaq_view.price * 100 / kosdaq_view.max_week_close  <=  week_markdown_percent.value + 5)
            & (kosdaq_view.price * 100 / kosdaq_view.max_week_close  >=  week_markdown_percent.value - 5)]
        else :
        selected = kosdaq_view.copy()

    if max_week_close_index.value != 0 :
       selected = selected[
           #price:max_week_close=x:100
           (selected.max_week_close_index <= max_week_close_index.value + 2)
           & (selected.max_week_close_index >= max_week_close_index.value - 2)]
    '''
    #selected = selected[selected.Genre.str.contains(genre_val)==True]
    #    print(selected.price * 100 / selected.max_week_close)
#    print(week_markdown_percent.value)
#    print(max_week_close_index.value)



def update(attrname, old, new):
    df = select_tickets()

    x_name = axis_map[x_axis.value]#price
    y_name = axis_map[y_axis.value]#stocks

    p.xaxis.axis_label = x_axis.value
    p.yaxis.axis_label = y_axis.value
    p.title = "%d tickets selected" % len(df)

    source.data = dict(
        x=df[x_name],
        y=df[y_name],
        color=df["color"],
        title=df["CompanyName"],
        code=df["SYMBOL"],
        alpha=df["alpha"],
    )
    print('it is updated')

#controls  = [week_period, strait_markup_weeks, strait_markdown_weeks, max_week_close_index, week_markdown_percent, x_axis, y_axis]
controls  = [strait_markup_weeks, strait_markdown_weeks, max_inst_vol, exchange_option, x_axis, y_axis]    
for control in controls:
    control.on_change('value', update)                

inputs = HBox(VBoxForm(*controls), width=300)

update(None, None, None) # initial load of the data

curdoc().add_root(HBox(inputs, p, width=1100))
