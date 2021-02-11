#!/usr/bin/env python3
"""
This file contains model template and implementation for Forecaster. All forecasting models should inherit from
ForecastModel, and override the _do_fit and _do_predict abstract methods
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple

import numpy as np
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler

from ..testing.util.constants import LOG


def get_models(model_args: Dict) -> Dict:
    """
    Retrieve a list of models based on names and init kwargs
    :param model_args:  dict of {
        <model_class_name>:  <kwargs dict>
    }
    :return: map of initialized model indexed by its class name
    """
    model_map = {}
    for name, kwargs in model_args.items():
        model_class = globals()[name]
        model = model_class(**kwargs)
        model_map[name] = model

    return model_map


class ForecastModel(ABC):
    """
    Interface for all the forecasting models
    """

    def __init__(self):
        self._x_transformer = None
        self._y_transformer = None
        pass

    @property
    def name(self):
        return self.__class__.__name__

    def fit(self, train_seqs: List[Tuple[np.ndarray, np.ndarray]]) -> None:
        """
        Fit the model with sequences
        :param train_seqs: List of training sequences and the expected output label in a certain horizon
        :return:
        """
        data = []
        for seq, _label in train_seqs:
            data = np.append(data, seq)
        data = data.reshape(-1, 1)
        self._x_transformer, self._y_transformer = self._get_transformers(data)

        if self._x_transformer or self._y_transformer:
            def norm_data(x):
                seq, label = x
                if self._x_transformer:
                    seq = self._x_transformer.transform(seq)
                if self._y_transformer:
                    label = self._y_transformer.transform(label)
                return seq, label
            # Use Map to save memory copy
            train_seqs = list(map(norm_data, train_seqs))

        self._do_fit(train_seqs)

    @abstractmethod
    def _do_fit(
            self, trains_seqs: List[Tuple[np.ndarray, np.ndarray]]) -> None:
        """
        Perform fitting.
        Should be overloaded by a specific model implementation.
        :param train_seqs: List of training sequences and the expected output label in a certain horizon. Normalization
            would have been done if needed
        :return:
        """
        raise NotImplementedError("Should be implemented by child classes")

    def predict(self, test_seq: np.ndarray) -> float:
        """
        Test a fitted model with a sequence.
        :param test_seq:  1D Test sequence
        :return: Predicted value at certain horizon
        """

        if self._x_transformer:
            test_seq = self._x_transformer.transform(test_seq)

        return self._do_predict(test_seq)

    @abstractmethod
    def _do_predict(self, test_seq: np.ndarray) -> float:
        """
        Perform testing.
        Should be overloaded by a specific model implementation.
        :param test_seq:  1D Test sequence
        :return: Predicted value at certain horizon
        """
        raise NotImplementedError("Should be implemented by child classes")

    @abstractmethod
    def _get_transformers(self, data: np.ndarray) -> Tuple:
        """
        Get the transformers
        :param data: Training data
        :return: A tuple of x and y transformers
        """
        raise NotImplementedError(
            "Each model should have its own transformers")


class LSTM(nn.Module, ForecastModel):
    """
    A simple LSTM model serves as a template for ForecastModel
    """

    def __init__(
            self,
            input_size: int = 1,
            hidden_layer_size: int = 100,
            output_size: int = 1,
            lr: float = 0.001,
            epochs: int = 10,
    ):
        """
        :param input_size: One data point that is fed into the LSTM each time
        :param hidden_layer_size:
        :param output_size: One output data point
        :param lr: learning rate while fitting
        :param epochs: number of epochs for fitting
        :param x_transformer: To transform the seq input
        :param y_transformer: To transform the label
        """
        nn.Module.__init__(self)
        ForecastModel.__init__(self)

        self._hidden_layer_size = hidden_layer_size

        self._lstm = nn.LSTM(input_size, hidden_layer_size)

        self._linear = nn.Linear(hidden_layer_size, output_size)

        self._hidden_cell = (torch.zeros(1, 1, self._hidden_layer_size),
                             torch.zeros(1, 1, self._hidden_layer_size))

        self._epochs = epochs
        self._lr = lr

    def forward(self, input_seq: torch.FloatTensor) -> float:
        """
        Forward propogation
        :param input_seq:  1D FloatTensor
        :return: A single value prediction
        """
        lstm_out, self._hidden_cell = self._lstm(
            input_seq.view(len(input_seq), 1, -1), self._hidden_cell)
        predictions = self._linear(lstm_out.view(len(input_seq), -1))
        return predictions[-1]

    def _do_fit(self, train_seqs: List[Tuple[np.ndarray, np.ndarray]]) -> None:
        """
        Perform training on the time series trace data.
        :param train_seqs: Training sequences of (seq, label)
        :return: None
        """
        epochs = self._epochs
        lr = self._lr

        # Training specifics
        loss_function = nn.MSELoss()
        optimizer = torch.optim.Adam(self.parameters(), lr=lr)
        LOG.info(f"Training with {len(train_seqs)} samples, {epochs} epochs:")
        for i in range(epochs):
            for seq, labels in train_seqs:
                optimizer.zero_grad()

                self._hidden_cell = (
                    torch.zeros(
                        1, 1, self._hidden_layer_size), torch.zeros(
                        1, 1, self._hidden_layer_size))

                seq = torch.FloatTensor(seq).view(-1)
                labels = torch.FloatTensor(labels).view(-1)

                y_pred = self(seq)

                single_loss = loss_function(y_pred, labels)
                single_loss.backward()
                optimizer.step()

            if i % 25 == 0:
                LOG.info(
                    f'[LSTM FIT]epoch: {i+1:3} loss: {single_loss.item():10.8f}')

        LOG.info(
            f'[LSTM FIT]epoch: {epochs:3} loss: {single_loss.item():10.10f}')

    def _do_predict(self, seq: np.ndarray) -> float:
        """
        Perform inference on a dataset. Returns a list of prediction results
        :param seq: Sequence for testing
        :return: Prediction results
        """
        # To tensor
        seq = torch.FloatTensor(seq).view(-1)

        with torch.no_grad():
            self._hidden = (torch.zeros(1, 1, self._hidden_layer_size),
                            torch.zeros(1, 1, self._hidden_layer_size))
            pred = self(seq)

        return pred.item()

    def _get_transformers(self, data: np.ndarray) -> Tuple:
        """
        Get the transformers
        :param data: Training data
        :return:  A tuple of x and y transformers
        """
        scaler = MinMaxScaler(feature_range=(-1, 1))
        scaler.fit(data)

        # Time-series data shares the same transformer
        return scaler, scaler
