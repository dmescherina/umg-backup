#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 09:45:23 2018

@author: usvyat
"""

import pandas as pd
import csv
import sys


def fill_nulls():
  df = pd.read_csv(sys.argv[1], sep='\t',header=None,error_bad_lines=False, index_col=False, dtype='unicode')
  df = df.fillna(r'\N')
  df.loc[:,df.dtypes==object].apply(lambda s:s.str.replace(" ", r'\N'))
  #df.ix[:,df.dtypes==object].apply(lambda s:s.str.replace('\t', ""))
  
  df.to_csv(sys.argv[2],sep='\t',header=None,index=False, quoting=csv.QUOTE_NONE)


  
if __name__ == "__main__":
    fill_nulls()

  

    
#fill_nulls("file:///Users/usvyat/Downloads/data%252Fapollo%252FQA%252Fdate_id=20180115%252Fapollo_export_20180115.csv","apollo_export_20180115")