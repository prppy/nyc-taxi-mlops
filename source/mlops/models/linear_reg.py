from sklearn.linear_model import LinearRegression


def train_linear(X_train, y_train):

    model = LinearRegression()
    model.fit(X_train, y_train)

    return model


def predict_linear(model, X):

    return model.predict(X)