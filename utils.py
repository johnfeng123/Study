import datetime
import numpy as np
import pandas as pd


def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)


def get_timeseries_by_column(df, col='atq',icol='datadate'):
    target_data =  df.loc[:,[icol, 'gvkey', col]]
    target_data = target_data.drop_duplicates([icol, 'gvkey'], keep='first')
    v = target_data.pivot(index=icol, columns='gvkey', values=col)
    return v

def how_many_multiple(df):
    tmp = df.groupby(['gvkey', 'datadate']).gvkey.count()
    tmp = tmp[tmp>1]
    tmp_index = tmp.index.values
    return tmp_index.size 

def filter_breakpt(df):
#    df.insert(df.shape[1],'diff',df['mcap']-df['breakpt'])
#    df['if_over_breakpt'] = df['diff'].apply(lambda x: 1 if x >=0 else 0)
#   temp = df.groupby(['gvkey']).if_over_breakpt.sum().reset_index()
#   tmp = temp[temp.if_over_breakpt==0]
#    small_mcap = tmp['gvkey']
#    df_new = df[~df.gvkey.isin(small_mcap)].drop(columns =['diff','if_over_breakpt'])
    temp = df[df['mcap'] >= df['breakpt']]
    key = temp.gvkey.unique()
    df_new = df[df.gvkey.isin(key)]
    return df_new

def get_time_series(df, col,icol='datadate'):

    
    if col not in df.columns:
        raise NameError("{col} does not exist".format(col=col))
    
    target_data =  df.loc[:, [icol, 'gvkey', col]]
    target_data = target_data.drop_duplicates([icol, 'gvkey', col], keep='first')
    #drop nan
    target_data = target_data[np.isnan(target_data[col]) == False]

    
    ## get multi data   
    tmp = target_data.groupby(['gvkey', 'datadate']).gvkey.count()
    tmp = tmp[tmp>1]
    tmp_index = tmp.index.values
    lala = target_data.reset_index(drop = True)
    multi = lala.loc[[0,1]]
     
    for i,j in tmp_index:
        df1 = target_data[(target_data.gvkey == i) & (target_data.datadate == j)]
        multi = multi.append(df1)
    
    multi =  multi.drop([0,1])
    
    multi =  multi.reset_index(drop = True)
    
    ## get not multi data
    multi_not = target_data.copy()
    
    for i,j in tmp_index:
        indexNames = multi_not[(multi_not.gvkey == i) & (multi_not.datadate == j)].index
        multi_not.drop(indexNames , inplace=True)
    	
    
    ##  if duplicate first-avarget/first < 0.05
    t1 = multi.groupby(['gvkey','datadate'])[col].first()
    ## change to average
    t2 = multi.groupby(['gvkey','datadate'])[col].mean()
    t3 = (t1-t2)/t1
    
    t4 = t3[abs(t3)<0.05].reset_index()
    ##check how many we drop
    print(col,len(t3)-len(t4),sep = " : ")
    
    ## drop value not close
    df3 = pd.merge(t4.drop([col], axis=1), t2.reset_index(), on=['gvkey', 'datadate'])
    
    target_data = multi_not.append(df3).sort_values(['gvkey', 'datadate'])
    
    v = target_data.pivot(index=icol, columns='gvkey', values=col)
    
    return v