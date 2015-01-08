import pandas as pd
from pandas import *

# copy paste table directly from the following url into a csv:
# http://vendor-reporting.wholefoods.com/microstrategy/asp/Main.aspx?evt=3067&src=Main.aspx.3067&visMode=0&reportID=557AE8C242636955B3B870A51822CC0A&reportViewMode=1&messageID=A0E5B42B42EBC3D95CDD9C98F8372202&executionMode=4&showOptionsPage=false&defaultRunMode=htmlGrids

df = read_csv('WFM.csv')
df.columns = 'delete4','region','store','storenum','delete','delete2','delete6','itemnum','product','size','delete3','delete5','week','units','storeselling','price'

df = df[['region','store','storenum','itemnum','product','size','week','units','storeselling','price']]

df.to_csv('wfm_output.csv')

stores = df[['region','store','storenum']].drop_duplicates().set_index('storenum')
stores.head()
stores.to_csv('stores.csv')

wfm_sales = df[['product','size','week','units','price','storeselling','storenum']]
wfm_sales.head()
wfm_sales['SKU'] = ''
for i in range(len(wfm_sales)):
    wfm_sales.loc[i,'SKU'] = wfm_sales.loc[i,'product'].upper() + ' ' + str(wfm_sales.loc[i,'size']) + 'OZ'

wfm_sales = wfm_sales[['week','units','price','storeselling','SKU','storenum']]
wfm_sales.loc[:,'SKU'] = wfm_sales.loc[:,'SKU'].apply(lambda x: x.replace('AND','&')) 

wfm_sales['year'] = wfm_sales.week.apply(lambda x: x.split(' ')[0])
wfm_sales['week'] = wfm_sales.week.apply(lambda x: x.split(' ')[-1])

wfm_sales['days'] = wfm_sales.week.apply(lambda x: 7*int(x))

import datetime
start_date = {2013: datetime.date(2012,10,1),
2014: datetime.date(2013,9,30),
2015: datetime.date(2014,9,29)
}

wfm_sales['start'] = wfm_sales.year.apply(lambda x: start_date[int(x)])

wfm_sales.loc[:, 'start_week'] = ''

for i in range(len(wfm_sales)):
    wfm_sales.loc[i, 'start_week'] = wfm_sales.loc[i,'start'] + datetime.timedelta(days = wfm_sales.loc[i,'days'])

wfm_sales = wfm_sales[['storenum','start_week','SKU','storeselling','units','price']]
wfm_sales.columns = 'storenum','week_starting','SKU','storeselling','units','price'

wfm_sales.to_csv('wfm_sales.csv')

wfm_sales['year'] = wfm_sales.week_starting.apply(lambda x: x.year)
wfm_sales['month'] = wfm_sales.week_starting.apply(lambda x: x.month)
wfm_sales['day'] = wfm_sales.week_starting.apply(lambda x: x.day)
wfm_sales = wfm_sales[['storenum','year','month','day','SKU','storeselling','units','price']]

wfm_sales.price = wfm_sales.price.apply(lambda x: x.replace('$',''))
wfm_sales.price = wfm_sales.price.apply(lambda x: x.replace('$',''))

wfm_sales.to_csv('wfm_sales.csv')
wfm_sales.head()
