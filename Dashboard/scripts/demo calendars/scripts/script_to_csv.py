import pandas as pd
import numpy as np
from pandas import *
import os
os.chdir('/home/jp/DATA/Dropbox/school/cloud/owlsbrew/Demo Calendars')

files = ['January','February','March','April','May','June','July','August','September']

for i in range(len(files)):
	df = read_csv('demo csv/' + files[i] + '.csv')

	calendar = {}
	for col in df.columns:
	    first = True
	    for row in df.index:
	        if first == True:
	            date = df.loc[row,col]
	            day = [i+1,col,date] # month, day, date
	            first = False
	        else:
	            if (type(date) == float or type(date) == np.float64) and np.isnan(date):
	                pass
	                first = True            
	            else:
	                date = int(date)
	                day.append(df.loc[row,col])
	                first = True
	                calendar[date] = day
	    if (type(date) == float or type(date) == np.float64) and np.isnan(date) or len(df) % 2 != 1:
	        pass
	    else:
	        day.append(float('nan'))
	        calendar[date] = day

	month = DataFrame.from_dict(calendar, orient = 'index')
	month.columns = 'Month','Day','Date','Info'

	month.loc[:,'Info'] = month.loc[:,'Info'].apply(lambda x: 
	        x if type(x) == float 
	        or type(x) == np.float64 
	        else x.decode('ascii', 'ignore'))
	month.Date = month.Date.apply(lambda x: int(x))
	month.sort('Date', inplace = True)

	if i == 0:
		all_demos = month
	else:
		all_demos = all_demos.append(month)
        
indx = range(len(all_demos))
all_demos['IDX'] = indx
all_demos.set_index('IDX', inplace = True)

demos = all_demos.copy()
demos.loc[:,'Info'] = demos.loc[:,'Info'].apply(lambda x: x if type(x) is float 
                                                        or type(x) is np.float64 
                                                        else x.split('/'))

newrows = {}
i = len(demos)
for row in demos.index:
    if type(demos.loc[row,'Info']) != float and type(demos.loc[row,'Info']) != np.float64:
        first = True
        for d in demos.loc[row,'Info']:
            if first == True:
                # store row
                curr_row = demos.loc[row,'Info']
                # insert in place
                demos.loc[row,'Info'] = d.strip()
                first = False
            else:
                newrows[i] =   [demos.loc[row,'Month'],
                                demos.loc[row,'Day'],
                                demos.loc[row,'Date'],
                                d.strip()]
                i += 1
                
                
newdays = DataFrame.from_dict(newrows, orient = 'index')
newdays.columns = demos.columns

demos = demos.append(newdays)
# demos.sort(['Month','Date'], ascending = True, inplace = True)
# demos.set_index(['Month','Date'], inplace = True)


# GET WFM WEEKS
demos['Year'] = 2014

import datetime
start_date = {2013: datetime.date(2012,10,1),
2014: datetime.date(2013,9,30),
2015: datetime.date(2014,9,29)
}

def inWeek(date):
    currdate = start_date[2013]
    when = 'before'
    while True:
        if (date - currdate).days < 0: # found
            return currdate - datetime.timedelta(days = 7)
        currdate += datetime.timedelta(days = 7)
        
demos['temp_date'] = ''        
for i in range(len(demos)):
    demos.loc[i, 'temp_date'] = datetime.date(int(demos.loc[i,'Year']),
                         int(demos.loc[i,'Month']),
                         int(demos.loc[i,'Date']))

 

# WFM promotion this day?
demos['WFM'] = False

def WFM(x):
	if type(x) is float or type(x) is np.float64:
		return False
	if 'WFM' in x.upper() or 'Whole Food' in x:
		return True
	return False

demos['wfm'] = False
demos.wfm = demos['Info'].apply(lambda x: WFM(x))



demos['temp_wfm'] = demos.temp_date.apply(lambda x: inWeek(x))

demos['wfm_year'] = 'NULL'
demos['wfm_month'] = 'NULL' 
demos['wfm_day'] = 'NULL'

for i in range(len(demos)):
	if demos.loc[i, 'wfm'] == True:
	    demos.loc[i, 'wfm_year'] = demos.loc[i, 'temp_wfm'].year
	    demos.loc[i, 'wfm_month'] = demos.loc[i, 'temp_wfm'].month
	    demos.loc[i, 'wfm_day'] = demos.loc[i, 'temp_wfm'].day

del demos['temp_wfm']
del demos['temp_date']

demos = demos[['Info','wfm','wfm_year','wfm_month','wfm_day','Year','Month','Date','Day']]
demos.columns = 'info','wfm','wfm_year', 'wfm_month', 'wfm_day', 'act_year', 'act_month', 'act_day', 'day_of_week'
demos.to_csv('./output/demos.csv')

import numpy as np
import math

string = 'INSERT INTO owlsbrew.demos_new VALUES'

for i in range(len(demos)):
    string += '(' + str(i) + ', '
    for c in demos.columns:
        if (type(demos.loc[i,c]) == float or type(demos.loc[i,c]) == np.float64) and math.isnan(demos.loc[i,c]):
            string += 'NULL, '
        elif c in ['wfm_year', 'wfm_month', 'wfm_day', 'act_year', 'act_month', 'act_day', 'wfm']:
            string += str(demos.loc[i,c]) + ','
        else:
        	# print demos.loc[i,c]
            string += " '" + demos.loc[i,c] + "', "
    string = string[:-2]
    string += '),'
string = string[:-1]   

print string

# create table demos_new(
# id int,
# info text,
# wfm boolean,
# wfm_year int,
# wfm_month int,
# wfm_day int,
# act_year int,
# act_month int,
# act_day int,
# day_of_week text,
# primary key(id)
# )