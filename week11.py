# -*- coding: utf-8 -*-
"""
Created on Sun Apr  7 16:12:01 2019

@author: Biao Feng
"""


import pandas as pd
import numpy as np
import utils
from DataLoader import DataLoader as DL
import matplotlib.pyplot as plt
%matplotlib inline

import seaborn as sns
import statsmodels.tsa.api as sta
import statsmodels.tsa.stattools as stattools
import statsmodels.tsa.api as smt
import statsmodels.api as sm
import statsmodels.stats.stattools as stools

import scipy.stats as stats

loc = 'c:/data/practicum/'
d = DL(loc)