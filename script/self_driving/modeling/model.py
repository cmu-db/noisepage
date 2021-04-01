#!/usr/bin/env python3

import numpy as np

import lightgbm as lgb

from sklearn import linear_model
from sklearn import kernel_ridge
from sklearn import ensemble
from sklearn import preprocessing
from sklearn import neural_network
from sklearn import multioutput
from sklearn import svm

# import warnings filter
from warnings import simplefilter

# ignore all future warnings
simplefilter(action='ignore', category=FutureWarning)

_LOGTRANS_EPS = 1e-4


def _get_base_ml_model(method):
    regressor = None
    if method == 'lr':
        regressor = linear_model.LinearRegression()
    if method == 'huber':
        regressor = linear_model.HuberRegressor(max_iter=50)
        regressor = multioutput.MultiOutputRegressor(regressor)
    if method == 'svr':
        regressor = svm.LinearSVR()
        regressor = multioutput.MultiOutputRegressor(regressor)
    if method == 'kr':
        regressor = kernel_ridge.KernelRidge(kernel='rbf')
    if method == 'rf':
        regressor = ensemble.RandomForestRegressor(n_estimators=50, n_jobs=8)
    if method == 'gbm':
        regressor = lgb.LGBMRegressor(max_depth=20, num_leaves=1000, n_estimators=100, min_child_samples=5,
                                      random_state=42)
        regressor = multioutput.MultiOutputRegressor(regressor)
    if method == 'nn':
        regressor = neural_network.MLPRegressor(hidden_layer_sizes=(25, 25), early_stopping=True,
                                                max_iter=1000000, alpha=5)

    return regressor


class Model:
    """
    The class that wraps around standard ML libraries.
    With the implementation for different normalization handlings
    """

    def __init__(self, method, normalize=True, log_transform=True, y_transformer=None, x_transformer=None):
        """

        :param method: which ML method to use
        :param normalize: whether to perform standard normalization on data (both x and y)
        :param log_transform: whether to perform log transformation on data (both x and y)
        :param y_transformer: the customized data transformer for output (a pair of functions with the first for
               training and second for predict)
        :param x_transformer: the customized data transformer for input
        """
        self._base_model = _get_base_ml_model(method)
        self._normalize = normalize
        self._log_transform = log_transform
        self._xscaler = preprocessing.StandardScaler()
        self._yscaler = preprocessing.StandardScaler()
        self._y_transformer = y_transformer
        self._x_transformer = x_transformer

    def train(self, x, y):
        if self._y_transformer is not None:
            y = self._y_transformer[0](x, y)

        if self._x_transformer is not None:
            x = self._x_transformer(x)

        if self._log_transform:
            x = np.log(x + _LOGTRANS_EPS)
            y = np.log(y + _LOGTRANS_EPS)

        if self._normalize:
            x = self._xscaler.fit_transform(x)
            y = self._yscaler.fit_transform(y)

        self._base_model.fit(x, y)

    def predict(self, x):
        original_x = x

        if self._x_transformer is not None:
            x = self._x_transformer(x)

        # transform the features
        if self._log_transform:
            x = np.log(x + _LOGTRANS_EPS)
        if self._normalize:
            x = self._xscaler.transform(x)

        # make prediction
        y = self._base_model.predict(x)

        # transform the y back
        if self._normalize:
            y = self._yscaler.inverse_transform(y)
        if self._log_transform:
            y = np.exp(y) - _LOGTRANS_EPS
            y = np.clip(y, 0, None)

        if self._y_transformer is not None:
            y = self._y_transformer[1](original_x, y)

        return y
