# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Process Mining Python Programm
# Hauptseminar Auditing Wintersemester 2020/2021
# Lisa Kaminski
# der Code wurde in Anlehnung an 'A Primer on Process Mining - Practical Skills with Python 
# and Graphviz' (2016), Diogo R.Ferreira erstellt.
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Deklaration der notwendigen Variablen
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import pandas as pd
import datetime as dt
import graphviz as gv
import os

FILENAME_SAVE = 'Logs_wild_path_test.dot'
FILENAME_DATA = '\\Logs_wild_path.csv'
DEFAULT_PATH = 'D:\\04-Studium\\Management & Economics, MSc\\02-Module\\01 - Auditing\\01 Working\\01 - Python'
DEFAULT_IMAGE = 'D:\\04-Studium\\Management & Economics, MSc\\02-Module\\01 - Auditing\\01 Working\\01 - Python\\03 - Images'
DEFAULT_LOGS = 'D:\\04-Studium\\Management & Economics, MSc\\02-Module\\01 - Auditing\\01 Working\\01 - Python\\01 - Logs'

DEFAULT_FONT = 'Arial'
DEFAULT_SIZE = '7'
DEFAULT_FONT_COLOR = 'gray10'
DEFAULT_COLOR = 'grey42'
els = "    "

Process_shown =   'case_frequency'#'throughput_time'

# LESEN DER .CSV DATEI
# Logdaten werden aus der .csv Datei extrahiert und als Dataframe eingelesen
# Anschließend Sortierung der Logdaten nach Case ID und Timestamp
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
parser = lambda Timestamp: dt.datetime.strptime(Timestamp, '%d.%m.%Y %H:%M')
Data = pd.read_csv(DEFAULT_LOGS + FILENAME_DATA, sep = ';', header = 0, parse_dates= ['Timestamp'], date_parser=parser).iloc[:,:]
Data.columns = ['case_id', 'Event', 'Event_name', 'User', 'Timestamp']
Data = Data.sort_values(by = ['case_id', 'Timestamp'])
print(Data)

# ERSTELLUNG DES CONTROL-FLOW
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cases = Data.drop_duplicates(subset = ['case_id'], keep = 'first', inplace = False, ignore_index = True).iloc[:,:1]
cases = cases['case_id'].tolist()
print("this are the different case_ids", cases)

# Ermittlung der Übergangszahl zwischen einzelnen Events
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

# Ermittlung der Anzahl an durchgeführten Events pro Event
A = dict()
for i in range(0, len(Data)):
    ai = Data['Event'][i][0]
    if ai not in A:
        A[ai] = 0
    A[ai] += 1

# Ermittlung der Übergangszeit zwischen einzelnen Events
Last_process = dict()
for case in cases:
    Data_case = Data[Data['case_id'] == case]
    Data_case.reset_index(drop = True, inplace = True)
    ai = Data_case['Event'].iloc[-1]
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
        avg_td -= dt.timedelta(microseconds=avg_td.microseconds)

        D[ai][aj] = avg_td
        print("the average duration is")
        print(ai, '->', aj, ':', avg_td)


# GRAFISCHE AUFBEREITUNG via graphviz
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dot = gv.Digraph(comment='Control-Flow', engine= 'dot', strict = True)

y_min = 0.5     # define minimal edge thickness
y_max = 3.0     # define maximal edge thickness

# Generierung eines separaten Dataframes zur Anzeige der Event Namen
event_name = Data[['Event', 'Event_name']].drop_duplicates(keep='first', ignore_index = True)

dot.attr('graph', rankdir = 'TB', nodesep = '0.2', ranksep = '0.2', pad = '1.5')
dot.attr('node', shape='box', style = 'rounded', fixedsize='shape', height='0.25', width = '1.1', penwidth='0.1', color=DEFAULT_COLOR, fontcolor=DEFAULT_FONT_COLOR, fontsize = DEFAULT_SIZE, fontname=DEFAULT_FONT)

dot.node('Process_start', label = els + 'Process start\l', rank = 'min',  shape = 'plaintext', image = DEFAULT_IMAGE + '//start_node.png', imagepos = 'ml', imagescale='true')
dot.node('Process_end', label = els +'Process end\l', rank = 'max', shape = 'plaintext', image = DEFAULT_IMAGE + '//end_node.png', imagepos = 'ml', imagescale='true')

values = [F[ai][aj] for ai in F for aj in F[ai]]

x_min = min(values)     # Minimum der Anzahl an Übergangen insgesamt
x_max = max(values)     # Maximum der Anzahl an Übergangen insgesamt

# Erstellung aller relevanten Nodes (Boxen) aus der Liste A
for ai in A:
    start_node = event_name[event_name['Event'] == str(ai)].iloc[0].iloc[1]
    w = len(els + start_node)    
    text = els + start_node +'\l' +  els + str(A[ai]) + '\l'
    if A[ai]/x_max > 0.8:
        img = 7
    elif A[ai]/x_max > 0.6:
        img = 6
    elif A[ai]/x_max > 0.4:
        img = 5
    elif A[ai]/x_max > 0.2:
        img = 4
    elif A[ai]/x_max > 0.0:
        img = 3
    
    dot.node(start_node, label = text, image = DEFAULT_IMAGE + '//Node_' + str(img) + '.png', imagepos = 'ml', imagescale='true', width = str(w/17 - 0.01))

if Process_shown == 'throughput_time':
    x_min = min([D[ai][aj] for ai in D for aj in D[ai]]).total_seconds()
    x_max = max([D[ai][aj] for ai in D for aj in D[ai]]).total_seconds()

# Erstellung aller relevanten Edges (Übergängen) der Liste F
for ai in F:    
    for aj in F[ai]:
        # Edge Breite
        x = F[ai][aj]

        if Process_shown == 'throughput_time':
            x = D[ai][aj].total_seconds()

        d = D[ai][aj].total_seconds()
        d = str(round(d/3600)) + str(' hours')
        
        try:
            y = y_min + (y_max-y_min) * float(x-x_min) / float(x_max-x_min)
        except:
            y = 3.0

        # Bestimmung von erster und letzter Node
        start_node = event_name[event_name['Event'] == str(ai)].iloc[0].iloc[1]
        end_node = event_name[event_name['Event'] == str(aj)].iloc[0].iloc[1]

        # Bestimmung der Farbe basierend auf der Anzahl an Übergängen zwischen Nodes  
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
            dot.edge('Process_start', start_node, penwidth = '3.0', color = '/pubu7/7', fontcolor=DEFAULT_COLOR, fontsize=str(int(DEFAULT_SIZE)-2), fontname=DEFAULT_FONT, style = 'dashed', arrowhead = 'none')

        if Process_shown == 'throughput_time':
            x = d
        if aj in Last_process:
            dot.edge(end_node, 'Process_end', penwidth = str(y), color = '/pubu7/'+ str(color), fontcolor=DEFAULT_COLOR, fontsize=str(int(DEFAULT_SIZE)-2), fontname=DEFAULT_FONT, style = 'dashed', dir = 'forward', arrowhead = 'none') 
        
        dot.edge(start_node, end_node, label = "  " + str(x), penwidth = str(y), color = '/pubu7/' + str(color), minlen = '1', fontcolor=DEFAULT_COLOR, fontsize=str(int(DEFAULT_SIZE)-2), fontname=DEFAULT_FONT, arrowhead = 'normal', arrowsize = '0.35')

# Prozess beenden, Datei speichern
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dot.save(filename = FILENAME_SAVE, directory = DEFAULT_PATH)
dot.render(filename = FILENAME_SAVE, directory= DEFAULT_PATH, format = 'pdf', view=False)
dot.view()

print("process finished")
