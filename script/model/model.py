#!/usr/bin/env python3

import numpy as np

import xgboost as xgb

from sklearn import linear_model
from sklearn import kernel_ridge
from sklearn import ensemble
from sklearn import preprocessing
from sklearn.neural_network import multilayer_perceptron
from sklearn import multioutput

_LOGTRANS_EPS = 1e-4


def _get_base_ml_model(method):
    regressor = None
    if method == 'lr':
        regressor = linear_model.LinearRegression()
    if method == 'kr':
        regressor = kernel_ridge.KernelRidge(kernel='rbf')
    if method == 'rf':
        regressor = ensemble.RandomForestRegressor(n_estimators=100, n_jobs=8)
    if method == 'xgb':
        regressor = xgb.XGBRegressor(max_depth=20, n_estimators=100, random_state=42)
    if method == 'nn':
        regressor = multilayer_perceptron.MLPRegressor(hidden_layer_sizes=(30, 30), early_stopping=True,
                                                       max_iter=1000000, alpha=0.01)

    multi_regressor = multioutput.MultiOutputRegressor(regressor)

    return multi_regressor


class Model:
    """
    The class that wraps around standard ML libraries.
    With the implementation for different normalization handlings
    """

    def __init__(self, method, normalize=True, log_transform=True):
        """

        :param method: which ML method to use
        :param normalize: whether to perform standard normalization on data (both x and y)
        :param log_transform: whether to perform log transformation on data (both x and y)
        """
        self._base_model = _get_base_ml_model(method)
        self._normalize = normalize
        self._log_transform = log_transform
        self._xscaler = preprocessing.StandardScaler()
        self._yscaler = preprocessing.StandardScaler()

    def train(self, x, y):
        if self._log_transform == 1:
            x = np.log(x + _LOGTRANS_EPS)
            y = np.log(y + _LOGTRANS_EPS)

        if self._normalize:
            x = self._xscaler.fit_transform(x)
            y = self._yscaler.fit_transform(y)

        self._base_model.fit(x, y)

    def predict(self, x):
        # transform the features
        if self._log_transform == 1:
            x = np.log(x + _LOGTRANS_EPS)
        if self._normalize:
            x = self._xscaler.fit_transform(x)

        # make prediction
        y = self._base_model.predict(x)

        # transform the y back
        if self._normalize:
            y = self._yscaler.inverse_transform(y)
        if self._log_transform == 1:
            y = np.exp(y) - _LOGTRANS_EPS
            y = np.clip(y, 0, None)

        return y
