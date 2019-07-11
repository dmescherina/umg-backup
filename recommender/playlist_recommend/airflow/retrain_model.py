#!/usr/bin/python
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

import sys

import pickle

from pitched_recommend import Recommender


def rertain_model():
    rec_config = {
        'pct_test': 0.1,
        'alpha': 100,
        'factors': 200,
        'regularization': 0.1,
        'iterations': 1000,
        'rseed': 4393971,
        'save_basedir': '../saved_models/iter1000_alpha100_factors200_reg01_rseed0_pctTest01',
        }


    rec = Recommender(rec_config)
    rec.build_recommender(sys.argv[1])
    with open(sys.argv[2], 'wb') as file:
    	pickle.dump(rec, file)

if __name__ == '__main__':
    rertain_model()