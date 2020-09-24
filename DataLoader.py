import pandas as pd
import numpy as np

import utils

class DataLoader:

    def __init__(self, loc):
        self.data_location = loc

        self.merged_table = None
        
        self.cols_q = ['acomincq','acoq','actq','apq',
                         'atq','capr1q','capr2q','capr3q','capsq','cheq','chq',
                       'cshtrq','cstkeq','cstkq','dlcq','dlttq','doq','dpq','dvpq',
                         'epsfiq','esubq','fcaq','gdwlq','icaptq','intanq',
                         'invtq','ivaeqq','ivaoq','lctq','ltq','mibtq',
                         'miiq','niq','nopiq','oiadpq','ppentq','pstknq','pstkrq',
                         'rectq','req','revtq','rllq','seqoq','seqq','spiq',
                         'teqq','tieq','tiiq','tstkq','txdiq','txditcq','txpq','txtq',
                        'uniamiq','utemq','wcapq','xintq','xiq','xoprq','xrdq'
                       #'ceqq'，'saleq'，'ibq'，'ltmibq',
                    ]

        self.load_data()
        self.merge()
        
        #self.get_timeseries(self,self.merged_table)



    def load_data(self):
        link_table = pd.read_csv('{loc}link table 1962-2001_csv.zip'.format(loc=self.data_location),
                                 header= 0,
                                 parse_dates=['datadate'],
                                 usecols=['datadate', 'GVKEY', 'LPERMNO', 'conm'])

        crsp = pd.read_csv('{loc}/crsp returns all 1962-2001_csv.zip'.format(loc=self.data_location),
                           header= 0,
                           parse_dates=['date'],
                           usecols=['date', 'PERMNO', 'PRC', 'RET', 'SHROUT', 'SHRCD', 'EXCHCD', 'PRIMEXCH', 'SHRCLS', 'TICKER'])

        compustat = pd.read_csv('{loc}/compustat_fundamentals qtr 1962-2001_csv.zip'.format(loc=self.data_location),
                                header= 0 ,
                                parse_dates=['datadate'],
                                usecols=['datadate', 'gvkey', 'exchg', 'fyearq', 'sic','datafqtr']
                                        + self.cols_q )

        self.data = {'link': link_table, 'crsp': crsp, 'compustat': compustat}
        self.load_mcap_breakpoints()
        self.good = 2

        self.process_data()
        self.apply_filter()


    def apply_filter(self):
        exchg_selected = [1,2,3]
        self.data['crsp'] = self.data['crsp'][self.data['crsp'].EXCHCD.isin(exchg_selected)]

        shrcd_selected = [10,11]
        self.data['crsp']= self.data['crsp'][self.data['crsp'].SHRCD.isin(shrcd_selected)]


    def process_data(self):
        # fill empty share class with NA
        self.data['crsp']['SHRCLS'] = self.data['crsp']['SHRCLS'].fillna('NA')

        # add market cap column to crsp
        self.data['crsp'].loc[:, 'mcap'] = np.abs(self.data['crsp']['PRC']) * self.data['crsp']['SHROUT']

        # add the month end dates
        vs = [utils.last_day_of_month(pd.to_datetime(uu)) for uu in self.data['crsp'].date.unique()]
        me_date_map = dict(zip(self.data['crsp'].date.unique(), vs))
        self.data['crsp'].loc[:, 'me_date'] = [me_date_map[uu] for uu in self.data['crsp'].date.values]

        # redefine the datadate : move one quarter forward
        all_dates = list(set(self.data['link'].datadate.values).union(self.data['compustat'].datadate.values))
        new_dates = [utils.last_day_of_month(pd.Timestamp(x) + pd.DateOffset(days=80)) for x in all_dates]
        date_map = dict(zip(all_dates, new_dates))

        self.data['link'].loc[:, 'datadate'] = [date_map[uu] for uu in self.data['link'].datadate.values]
        self.data['compustat'].loc[:, 'datadate'] = [date_map[uu] for uu in self.data['compustat'].datadate.values]


    def merge(self, reload=False):
        if self.merged_table is None or reload:
            #change columns name to make sure column names are same in both database
            linktable = self.data['link'].rename(columns = {'GVKEY':'gvkey'})

            #aggregate returns quarterly (crsp data has monthly returns)
            c = self.preaggregate_crsp_table(self.data['crsp'])

            #merge compustat and link table
            tmp = pd.merge(c,
                           linktable ,
                           how ='inner',
                           left_on=['PERMNO', 'me_date'],
                           right_on= ['LPERMNO', 'datadate'])

            tmp = self.aggregate_crsp_table(tmp)

            #merge with compustat
            merged = pd.merge(self.data['compustat'],
                              tmp,
                              how ='inner',
                              on=['gvkey', 'datadate'])

            self.merged_table = pd.merge(merged,
                                         self.mcap_breakpoints,
                                         how='left',
                                         left_on='datadate',
                                         right_on='mcap_date')
            
            ##filter break point
            self.merged_table = self.filter_breakpt(self.merged_table)

            ##drop all duplicate
            tmp = self.merged_table.groupby(['gvkey', 'datadate']).gvkey.count()
            multi_count = tmp[tmp==1].rename("count").reset_index()
            self.merged_table = pd.merge(self.merged_table,multi_count).drop("count",axis=1)

            ## industry classifer
            self.merged_table = self.industry_classifier(self.merged_table)
            
    def load_mcap_breakpoints(self):
        # FAMA FRENCH NYSE ME breakpoints
        ff_me_breakpoints = pd.read_csv('{loc}FamaFrench_ME_Breakpoints_csv.zip'.format(loc=self.data_location),
                                        skiprows=[0],
                                        header=-1,
                                        names=['date', 'count'] + [5*x for x in np.arange(1,21)]).iloc[:-1]
        dts = [utils.last_day_of_month(pd.Timestamp('%s-%s-01' % (x[:4], x[4:6]))) for x in ff_me_breakpoints.date.values]
        ff_me_breakpoints.loc[:, 'date'] = dts

        self.mcap_breakpoints = ff_me_breakpoints[['date', 20]]
        self.mcap_breakpoints.columns = ['mcap_date', 'breakpt']
        self.mcap_breakpoints.loc[:, 'breakpt'] = self.mcap_breakpoints['breakpt'] * 1000  # to be consistent with the 'mcap' column





    @staticmethod
    def aggregate_crsp_table(df):
        ## drop duplicate first
        df = df.drop_duplicates(['datadate', 'SHRCLS', 'gvkey', 'mcap', 'QRET', 'TICKER'])
       
        aggr_mcap = df.groupby(['gvkey', 'datadate']).mcap.sum().reset_index()
        df = df.rename(columns={'QRET': 'QRET_indi'})
        df3 = pd.merge(df, aggr_mcap, on=['gvkey', 'datadate'], how ='inner', suffixes=('', '_aggr'))
                 
        df3 = df3.assign(mcap_weight=lambda row: row.mcap / row.mcap_aggr)
        
        tmp = df3.groupby(['PERMNO', 'SHRCLS','datadate'])['mcap_weight'].last().unstack(-1).T.sort_index()
        tmp = tmp.shift(1)
        tmp = tmp.T.stack().reset_index().rename(columns={0: 'mcap_weight'})
        
        df3 = pd.merge(df3.drop(['mcap_weight'], axis=1), tmp, on=['PERMNO', 'SHRCLS', 'datadate'])
        
        ## add one row RET
        df3 = df3.assign(RET=lambda row: row.QRET_indi * row.mcap_weight)
        
        new_df = pd.merge(df3.groupby(['gvkey', 'datadate']).RET.sum().reset_index(), aggr_mcap, on=['gvkey', 'datadate'])

        return new_df


    @staticmethod
    def preaggregate_crsp_table(df):
        c = df.drop_duplicates()
        c = c[c.RET != 'C']

        crsp_rets = c.groupby(['PERMNO', 'SHRCLS', 'me_date']).RET.last()

        crsp_rets = crsp_rets.unstack(level=-1).T
        crsp_rets = crsp_rets.astype(np.float32)

        # aggregate at the end of each quarter, calculate compounded returns
        crsp_rets = (1+crsp_rets).resample('Q').prod()-1

        crsp_rets = crsp_rets.T.stack().reset_index().rename(columns={0: 'QRET'})

        # c has an additional column 'QRET', which is the return you want to use.
        c = pd.merge(c, crsp_rets, on=['PERMNO', 'SHRCLS', 'me_date'])

        return c


    @staticmethod
    def industry_classifier(df):
        df['i1'] = df['sic'].apply(lambda x: 1 if (x >=1000) & (x <=1499) else 0)
        df['i2'] = df['sic'].apply(lambda x: 1 if (x >=1500) & (x <=1799) else 0)
        df['i3'] = df['sic'].apply(lambda x: 1 if (x >=2000) & (x <=3999) else 0)
        df['i4'] = df['sic'].apply(lambda x: 1 if (x >=4000) & (x <=4999) else 0)    
        df['i5'] = df['sic'].apply(lambda x: 1 if (x >=5000) & (x <=5199) else 0)
        df['i6'] = df['sic'].apply(lambda x: 1 if (x >=5200) & (x <=5999) else 0)
        df['i7'] = df['sic'].apply(lambda x: 1 if (x >=6000) & (x <=6799) else 0)
        df['i8'] = df['sic'].apply(lambda x: 1 if (x >=7000) & (x <=8999) else 0)
        ##Exclude the 4 industries
        indexNames = df[((df['sic'] >= 100)&(df['sic'] <= 999))|(df['sic'] >= 9900)&(df['sic'] <=9999)].index
        df.drop(indexNames)
        
        return df
  
    @staticmethod
    def get_timeseries(self,df):
        
        for i in range(0,len(self.cols_q)):
            globals()[self.cols_q[i]] = self.get_time_series(self.merged_table,self.cols_q[i])


            
    @staticmethod
    def get_time_series(df, col,icol='datadate'):
        if col not in df.columns:
            raise NameError("{col} does not exist".format(col=col))
            
        target_data =  df.loc[:, [icol, 'gvkey', col]]
        target_data = target_data.drop_duplicates([icol, 'gvkey', col], keep='first')
        

        ## get multi data & multi not data
        tmp = target_data.groupby(['gvkey', 'datadate']).gvkey.count()
        multi_count = tmp[tmp>1].rename("count").reset_index()
        multi_not_count = tmp[tmp==1].rename("count").reset_index()
        
        multi = pd.merge(target_data,multi_count).drop("count",axis=1)
        multi_not = pd.merge(target_data,multi_not_count).drop("count",axis=1)
        
        
        if multi.gvkey.count() > 0:
            ##drop nan
            x = multi[np.isnan(multi[col]) == False]
        
            ## after drop nan, divide multi to x_multi and x_multi not
            ## let x_multi become new multi, x_multi_not + multi_not become new multi_not
            tmp = x.groupby(['gvkey', 'datadate']).gvkey.count()
            x_multi_count = tmp[tmp>1].rename("count").reset_index()
            x_multi_not_count = tmp[tmp==1].rename("count").reset_index()
            x_multi = pd.merge(x, x_multi_count).drop("count",axis=1)
            x_multi_not = pd.merge(x, x_multi_not_count).drop("count",axis=1)
            multi = x_multi
            multi_not = multi_not.append(x_multi_not)
        
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
 
    
    
    @staticmethod
    def filter_breakpt(df):
   
        temp = df[df['mcap'] >= df['breakpt']]
        key = temp.gvkey.unique()
        df_new = df[df.gvkey.isin(key)]
        return df_new