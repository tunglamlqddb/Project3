import pandas as pd
import sklearn, os, json, numpy as np
from sklearn.model_selection import train_test_split
from sklearn import svm, metrics
from sklearn.preprocessing import StandardScaler


basedir = os.path.dirname((os.path.dirname(__file__)))
results_path = os.path.join(basedir, 'Results')

# def load_data():

# def split_data():

def train(data_name, test_percent):
    test_percent = int(test_percent)
    data_path = results_path + "/" + data_name
    data = pd.read_csv(data_path)
    
    data = data.drop_duplicates(subset=['CommonNeighbor', 'AdamicAdar', 'JaccardCoefficient', 'PreferentialAttachment', 'ResourceAllocation', 'ShortestPath' ,'CommonCountry', 'Label'])
    
    X = data.drop(columns=['id_author_1', 'id_author_2', 'Label'])
    y = data['Label']

    scaler = StandardScaler().fit(X)
    X = scaler.transform(X)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=float(test_percent/100), random_state=608, shuffle=True)

    print("Tỉ lệ nhãn 0-1")
    print("Train")
    print(np.sum(y_train==0), end='--')
    print(np.sum(y_train==1))
    print("Test")
    print(np.sum(y_test==0), end='--')
    print(np.sum(y_test==1)) 

    model = svm.SVC(kernel='rbf', max_iter=5000)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    print("Precision:",metrics.precision_score(y_test, y_pred))
    print("Recall:",metrics.recall_score(y_test, y_pred))
    print("F1 score:",metrics.f1_score(y_test, y_pred))
    print("Roc_auc_score score:",metrics.roc_auc_score(y_test, y_pred))
    
    result = {}
    result['Precision'] = metrics.precision_score(y_test, y_pred)
    result['Recall'] = metrics.recall_score(y_test, y_pred)
    result['f1_score'] = metrics.f1_score(y_test, y_pred)
    result['Roc_auc'] = metrics.roc_auc_score(y_test, y_pred)

    return json.dumps({"results": result})


