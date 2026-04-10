import xgboost as xgb


def train_xgb(X_train, y_train):

    model = xgb.XGBRegressor(
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1,
    random_state=42
    )

    model.fit(X_train, y_train)

    return model


def predict_xgb(model, X):

    return model.predict(X)