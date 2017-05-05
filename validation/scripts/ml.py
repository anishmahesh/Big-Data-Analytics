import csv
import sklearn.linear_model as liner_model
import sklearn.ensemble as ensemble
from sklearn.model_selection import train_test_split

import numpy as np

def getData(read_file_name):
    csv_reader = csv.reader(open(read_file_name, "rt"))
    return csv_reader


def trainModel(X_train, y_train, type = liner_model.LinearRegression()):
    model = type
    model.fit(X_train, y_train)
    return model

def testModel(model, X_test):
    return model.predict(X_test)


def meanSquareError(prediction, y_test):
    return np.mean((prediction - y_test) ** 2)


def comparision(prediction, y_test):
    for i in range(len(prediction)):
        print(prediction[i], y_test[i])


def main():
    csv_reader = getData(read_file_name='../../RTBDA/Unique_Values/merged.csv')
    data = np.array(list(csv_reader)).astype("int")
    X, y = np.append(data[:,:13], data[:,14:],1), data[:,13]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.02, random_state=42)
    model = trainModel(X_train, y_train, ensemble.RandomForestRegressor() )
    prediction = testModel(model, X_test)
    mse = meanSquareError(prediction, y_test)
    print(mse)
    comparision(prediction,y_test)

if __name__ == '__main__':
    main()
