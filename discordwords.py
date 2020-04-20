#!/usr/bin/python3
from collections import defaultdict
from glob import glob
import re
import sys
import pandas as pd
import plotly.graph_objects as go
import argparse
import json
import os

ps = argparse.ArgumentParser(description='Parses and presents data from Discord\'s data dump')
ps.add_argument("path", type=str, help='Path to folder in which Discord\'s data is held. Will contain a /messages/ folder')
ps.add_argument("-c", "--cloud", action="store_true", help="Present data as word cloud")
ps.add_argument("-b", "--bar", action="store_true", help="Present data as bar chart (default)")
ps.add_argument("-t", "--time", action="store_true", help="Present data as a timeseries")
ps.add_argument("-n", "--num", type=int, nargs="+", help="Number of words to display. Bar chart defaults to 40, WordCloud defaults to 512")
ps.add_argument("-s", "--start", type=str, nargs="+", help="Starting date (year-month-day) Defaults to beginning of data")
ps.add_argument("-e", "--end", type=str, nargs="+", help="Stop date (year-month-day) Defaults to end of data")

args=ps.parse_args()
regex = re.compile('[^a-zA-Z]')

assert os.path.isdir(args.path)
messages_path = os.path.join(args.path, "messages")

channels = glob(os.path.join(messages_path, "*", "messages.csv"))
if len(channels)<1:
	print(messages_path + " is not readable")
	exit()

with open(os.path.join(args.path, "servers", "index.json")) as f:
    servers = json.load(f).values()

messages=[]
uwords = defaultdict(int)
twords=[]
acsv = pd.concat([pd.read_csv(str(i), usecols=[1, 2]) for i in channels])
acsv['Timestamp'] = pd.to_datetime(acsv['Timestamp'])

sdate=min(acsv['Timestamp'])
edate=max(acsv['Timestamp'])

if args.start is not None:
	sdate=args.start[0]
if args.end is not None:
	edate=args.end[0]
acsv=(acsv.loc[(acsv['Timestamp'] > sdate) & (acsv['Timestamp'] <= edate)])

for channel in acsv.to_numpy():
	for word in str(channel[1]).split():
		cleanedword=regex.sub('', word).strip().lower()
		if cleanedword:
			uwords[cleanedword] += 1

for tple in uwords:
	twords.append((tple, uwords[tple]))
twords=sorted(twords, key=lambda x: x[1])

if args.num is not None:
	nmax=args.num[0]
else:
	if args.cloud:
		nmax=512
	else:
		nmax=40
if nmax>len(twords) or nmax==-1:
	nmax=len(twords)

if args.cloud:
	from wordcloud import WordCloud
	import matplotlib.pyplot as plt
	print(str(str(len(twords))+' words selected over '+str(len(servers))+' servers.<br>'+
		   'Date range: '+str(pd.Timestamp(sdate).date())+' - '+str(pd.Timestamp(edate).date())+'\n'))
	wordcloud = WordCloud(width=1920,height=1080, max_words=nmax,relative_scaling=1,normalize_plurals=False).generate_from_frequencies(dict(twords))
	plt.imshow(wordcloud, interpolation='bilinear')
	plt.axis("off")
	plt.show()

if args.time:
	tt=str(str(len(twords))+' words selected over '+str(len(servers))+' servers.<br>'+
	   'Date range: '+str(pd.Timestamp(sdate).date())+' - '+str(pd.Timestamp(edate).date())+'<br> ')
	acsv['Timestamp'] = pd.to_datetime(acsv['Timestamp']).dt.normalize()      #remove time, keep date
	del acsv['Contents']                                                      #remove contents of message
	acsv['Count'] = acsv.groupby('Timestamp')['Timestamp'].transform('count') #create count col
	acsv=acsv.drop_duplicates(keep="first").sort_values('Timestamp')          #drop duplicate dates

	acsv['Timestamp'] = pd.to_datetime(acsv['Timestamp'])
	acsv.set_index('Timestamp', inplace=True)
	acsv=acsv.resample('D').mean().reset_index().fillna(0)
	fig = go.Figure()
	fig.add_trace(go.Scatter(x=list(acsv['Timestamp']), y=list(acsv['Count'])))
	fig.update_layout(
	    title_text=tt
	)

	fig.update_layout(
	    xaxis=dict(
	        rangeselector=dict(
	            buttons=list([
	                dict(count=1,
	                     label="1d",
	                     step="day",
	                     stepmode="backward"),
	                dict(count=1,
	                     label="1m",
	                     step="month",
	                     stepmode="backward"),
	                dict(count=1,
	                     label="1y",
	                     step="year",
	                     stepmode="backward"),
	                dict(step="all")
	            ])
	        ),
	        rangeslider=dict(
	            visible=True
	        ),
	        type="date"
	    )
	)	

	fig.show()


if not args.cloud and not args.time:
	tt=str(str(len(twords))+' words selected over '+str(len(servers))+' servers.<br>'+
		   'Date range: '+str(pd.Timestamp(sdate).date())+' - '+str(pd.Timestamp(edate).date())+'<br>'+
		   'Unique words presented: '+str(nmax))
	x = [i[0] for i in twords[-nmax:]]
	y = [i[1] for i in twords[-nmax:]]
	pl = go.Bar(
	    x=x,
	    y=y,
	    )
	layout = go.Layout(
	    title=tt,
	    xaxis=dict(
	        title='Word'
	    ),
	    yaxis=dict(
	        title='Count'
	    ),
	    hovermode='closest',
	    #showlegend=True
	)
	figure = go.Figure(pl, layout=layout)
	figure.show()