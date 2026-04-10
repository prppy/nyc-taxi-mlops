import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense


def create_sequences(X, y, window=24):

    X_seq, y_seq = [], []

    for i in range(len(X) - window):
        X_seq.append(X[i:i + window])
        y_seq.append(y[i + window])

    return np.array(X_seq), np.array(y_seq)


def train_lstm(X_train, y_train):

    X_seq, y_seq = create_sequences(X_train.values, y_train.values)

    model = Sequential()
    model.add(LSTM(64, input_shape=(X_seq.shape[1], X_seq.shape[2])))
    model.add(Dense(1))

    model.compile(optimizer="adam", loss="mse")

    model.fit(X_seq, y_seq, epochs=5, batch_size=32, verbose=1)

    return model