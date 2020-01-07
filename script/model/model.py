#!/usr/bin/env python3

import numpy as np

import xgboost as xgb

from sklearn.linear_model import LinearRegression
from sklearn.kernel_ridge import KernelRidge
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.neural_network.multilayer_perceptron import MLPRegressor
from sklearn.multioutput import MultiOutputRegressor

LOGTRANS_EPS = 1e-4

def _get_base_ML_model(method):
    if method == 'lr':
        regressor = LinearRegression()
    if method == 'kr':
        regressor = KernelRidge(kernel='rbf')
    if method == 'rf':
        regressor = RandomForestRegressor(n_estimators=100, n_jobs=8)
    if method == 'xgb':
        regressor = xgb.XGBRegressor(max_depth=20, n_estimators=100, random_state=42)
    if method == 'nn':
        regressor = MLPRegressor(hidden_layer_sizes=(30, 30), early_stopping=True, max_iter=1000000, alpha=0.01)

    multi_regressor = MultiOutputRegressor(regressor)

    return multi_regressor

class Model:
    '''
    The class that wraps around standard ML libraries.
    With the implementation for different normalization handlings
    '''
    def __init__(self, method, normalize = True, log_transform = True):
        '''

        :param method: which ML method to use
        :param normalize: whether to perform standard normalization on data (both x and y)
        :param log_transform: whether to perform log transformation on data (both x and y)
        '''
        self._base_model = _get_base_ML_model(method)
        self._normalize = normalize
        self._log_transform = log_transform
        self._xscaler = StandardScaler()
        self._yscaler = StandardScaler()


    def train(self, x, y):
        if self._log_transform == 1:
            x = np.log(x + LOGTRANS_EPS)
            y = np.log(y + LOGTRANS_EPS)

        if self._normalize == True:
            x = self._xscaler.fit_transform(x)
            y = self._yscaler.fit_transform(y)

        self._base_model.fit(x, y)

    def predict(self, x):
        # transform the features
        if self._log_transform == 1:
            x = np.log(x + LOGTRANS_EPS)
        if self._normalize == True:
            x = self._xscaler.fit_transform(x)

        # make prediction
        y = self._base_model.predict(x)

        # transform the y back
        if self._normalize == True:
            y = self._yscaler.inverse_transform(y)
        if self._log_transform == 1:
            y = np.exp(y) - LOGTRANS_EPS
            y = np.clip(y, 0, None)

        return y

