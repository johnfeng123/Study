# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 16:23:49 2019

@author: Biao Feng
"""

from DataLoader import DataLoader as DL
from DataLoader18 import DataLoader as DL18
import pandas as pd
loc = 'C:/Users/Biao Feng/source/Anaconda3/practicum/'
d = DL(loc)
d18 = DL18(loc)
universe = pd.concat([d.merged_table, d18.merged_table]).sort_values(['gvkey','datadate']).drop_duplicates(['gvkey','datadate'], keep = 'first').ix[:,1:]
universe.to_csv('universe')
