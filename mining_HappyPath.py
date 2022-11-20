# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Process Mining Python Programm
# Hauptseminar Auditing Wintersemester 2020/2021
# Lisa Kaminski
# for code reference see 
#   'A Primer on Process Mining 
#      - Practical Skills with Python and Graphviz' (2016), Diogo R.Ferreira
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import pandas as pd
import datetime as dt
import graphviz as gv
import os

FILENAME_SAVE = 'Logs_happy_path - Kopie.dot'
FILENAME_DATA = '\\Logs_happy_path - Kopie.csv'
DEFAULT_PATH = 'D:\\04-Studium\\Management & Economics, MSc\\02-Module\\01 - Auditing\\01 Working\\01 - Python'
DEFAULT_IMAGE = 'D:\\04-Studium\\Management & Economics, MSc\\02-Module\\01 - Auditing\\01 Working\\01 - Python\\03 - Images'
DEFAULT_LOGS = 'D:\\04-Studium\\Management & Economics, MSc\\02-Module\\01 - Auditing\\01 Working\\01 - Python\\01 - Logs'

DEFAULT_FONT = 'Arial'
DEFAULT_SIZE = '9'
DEFAULT_FONT_COLOR = 'gray10'
DEFAULT_COLOR = 'grey42'

els = "      " #empty label space
Process_shown = 'case_frequency' #'throughput_time'

# READING DATA
# obtaining the log data from the corresponding excel .csv file
# and sort values according to case_id and timestamp to enable modelling of the process
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
parser = lambda Timestamp: dt.datetime.strptime(Timestamp, '%d.%m.%Y %H:%M:%S')
Data = pd.read_csv(DEFAULT_LOGS + FILENAME_DATA, sep = ';', header = 0, parse_dates= ['Timestamp'], date_parser=parser).iloc[:,:]
Data.columns = ['case_id', 'Event', 'Event_name', 'User', 'Timestamp']
Data = Data.sort_values(by = ['case_id', 'Timestamp'])
print(Data)

# CREATE CONTROL-FLOW
# prepare loop for counting the process transformation
# then loop through all different cases, saving the single process transformations
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cases = Data.drop_duplicates(subset = ['case_id'], keep = 'first', inplace = False, ignore_index = True).iloc[:,:1]
cases = cases['case_id'].tolist()
print("this are the different case_ids", cases)

# obtain number of transactions between each node
F = dict()
for case in cases:
    for i in range(0, len(Data[Data['case_id'] == case])-1):
        try:
            Data_case = Data[Data['case_id'] == case]
            Data_case.reset_index(drop = True, inplace = True)
            ai = Data_case['Event'][i][0]
            aj = Data_case['Event'][i+1][0]
            if ai not in F:
                F[ai] = dict()
            if aj not in F[ai]:
                F[ai][aj] = 0
            F[ai][aj] +=1
        except:
            pass

for ai in sorted(F.keys()):
    for aj in sorted(F[ai].keys()):
        print(ai, '->', aj, ':', F[ai][aj])

# obtain number of conducted items with every event
A = dict()
for i in range(0, len(Data)):
    ai = Data['Event'][i][0]
    if ai not in A:
        A[ai] = 0
    A[ai] += 1

#import collections
# obtain the timestamp difference between the events
Last_process = dict()
for case in cases:
    Data_case = Data[Data['case_id'] == case]
    Data_case.reset_index(drop = True, inplace = True)
    ai = Data_case['Event'].iloc[-1]
    print(ai)
    if ai not in Last_process:
        Last_process[ai] = dict()
        
D = dict()
for case in cases:
    for i in range(0, len(Data[Data['case_id'] == case])-1):
        Data_case = Data[Data['case_id'] == case]
        Data_case.reset_index(drop = True, inplace = True)
        ai = Data_case['Event'][i][0]
        ti = Data_case['Timestamp'][i]
        aj = Data_case['Event'][i+1][0]
        tj = Data_case['Timestamp'][i+1]
        if ai not in D:
            D[ai] = dict()
        if aj not in D[ai]:
            D[ai][aj] = []

        duration = (tj-ti)
        D[ai][aj].append(duration)

for ai in sorted(D.keys()):
    for aj in sorted(D[ai].keys()):
        sum_td = sum(D[ai][aj], dt.timedelta())
        count_td = len(D[ai][aj])
        avg_td = sum_td/count_td
        avg_td -= dt.timedelta(days=avg_td.days) # FORMAT ANPASSEN

        print("the average duration is")
        print(ai, '->', aj, ':', avg_td)

# GRAFICAL INTERPRETATION
# via graphviz
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dot = gv.Digraph(comment='Control-Flow', engine= 'dot', strict = True)

y_min = 0.5     # define minimal edge thickness
y_max = 3.0     # define maximal edge thickness

# generate a separate dataframe to enable the view of the actual event names instead of their variables as in column event
event_name = Data[['Event', 'Event_name']].drop_duplicates(keep='first', ignore_index = True)

dot.attr('graph', rankdir = 'TB', nodesep = '0.35', ranksep = '0.1', splines = 'line')
dot.attr('node', image = DEFAULT_IMAGE + '//Node_7.png', imagepos = 'ml', imagescale='true', shape='box', style = 'rounded', fixedsize='shape', height='0.3', width = '1.35', penwidth='0.1', color=DEFAULT_COLOR, fontcolor=DEFAULT_FONT_COLOR, fontsize = DEFAULT_SIZE, fontname=DEFAULT_FONT)

dot.node('Process_start', label = els + 'Process Start\l', rank = 'min',  shape = 'plaintext', image = DEFAULT_IMAGE + '//start_node.png', imagepos = 'ml', imagescale='true')
dot.node('Process_end', label = els +'Process End\l', rank = 'max', shape = 'plaintext', image = DEFAULT_IMAGE + '//end_node.png', imagepos = 'ml', imagescale='true')

if Process_shown == 'case_frequency':
    values = [F[ai][aj] for ai in F for aj in F[ai]]
    x_min = min(values)     # define minimal number of transitions
    x_max = max(values)     # define maximal number of transitions 

    for ai in A:
        start_node = event_name[event_name['Event'] == str(ai)].iloc[0].iloc[1]    
        text = els + start_node +'\l' +  els + str(A[ai]) + '\l'
        #text ='"<<FONT POINT-SIZE="9">"' + els + start_node +'"</FONT>\l' + '<FONT POINT-SIZE="7">"' + str(A[ai])+ '"</FONT>>\l'
        dot.node(start_node, label = text)
        #dot.node(start_node, lp = '-10', label ='<<FONT POINT-SIZE="9">' + els + start_node +'</FONT>\l' + '<FONT POINT-SIZE="7">' + str(A[ai])+ '</FONT>>\l')


    for ai in F:
        for aj in F[ai]:
            # node thickness
            x = F[ai][aj]
            try:
                y = y_min + (y_max-y_min) * float(x-x_min) / float(x_max-x_min)
            except:
                y = 3.0

            # determine label of start and end node
            start_node = event_name[event_name['Event'] == str(ai)].iloc[0].iloc[1]
            end_node = event_name[event_name['Event'] == str(aj)].iloc[0].iloc[1]

            # determine color of the edge depending on the amount of flows  
            if x/x_max > 0.8:
                color = 7
            elif x/x_max > 0.6:
                color = 6
            elif x/x_max > 0.4:
                color = 5
            elif x/x_max > 0.2:
                color = 4
            elif x/x_max > 0.0:
                color = 3

            if ai == 'a':
                dot.edge('Process_start', start_node, penwidth = str(y), color = '/pubu7/4', fontcolor=DEFAULT_COLOR, fontsize=str(int(DEFAULT_SIZE)-2), fontname=DEFAULT_FONT, style = 'dashed', arrowhead = 'none')

            if aj in Last_process:
                dot.edge(end_node, 'Process_end', label = "  " + str(F[ai][aj]), penwidth = str(y), color = '/pubu7/4', fontcolor=DEFAULT_COLOR, fontsize=str(int(DEFAULT_SIZE)-2), fontname=DEFAULT_FONT, style = 'dashed', dir = 'forward', arrowhead = 'none') 
        
            dot.edge(start_node, end_node, label = "  " + str(F[ai][aj]), penwidth = str(y), color = '/pubu7/4', minlen = '1', fontcolor=DEFAULT_COLOR, fontsize=str(int(DEFAULT_SIZE)-2), fontname=DEFAULT_FONT, arrowhead = 'normal', headport = 'n', tailport = 's')

dot.save(filename = FILENAME_SAVE, directory = DEFAULT_PATH)
dot.render(filename = FILENAME_SAVE, directory= DEFAULT_PATH, format = 'pdf', view=False)
dot.view()


print("process finished")
