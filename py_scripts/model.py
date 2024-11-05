import pandas as pd
import numpy as np
from sklearn.model_selection import KFold, cross_validate
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import FunctionTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import RandomizedSearchCV

dataset = pd.read_csv('final_dataset.csv.gz', compression='gzip')
labels = pd.read_csv('labels.csv.gz', compression='gzip')
dataset = pd.merge(dataset, labels, how='left', left_on='hex_id', right_on='hex_id')
dataset = dataset.set_index('hex_id').astype('float')

train = dataset[~dataset.cost_of_living.isna()]
test = dataset[dataset.cost_of_living.isna()]

X_train = train[[c for c in train if c != 'cost_of_living']].values
X_test = test[[c for c in test if c != 'cost_of_living']].values
y_train = train[['cost_of_living']].values.reshape(len(train), -1)
y_test = test[['cost_of_living']].values.reshape(len(test), -1)


numeric_cols = [c for c in train if c != 'cost_of_living' and 'is_' not in c]


# preprocessor = ColumnTransformer(
#     transformers=[
#         ('num', StandardScaler(), numeric_cols)
#     ])

# Pipeline base
model = Pipeline([
#    ('preprocessor', preprocessor),
    ('regressor', XGBRegressor())
])

# Parámetros expandidos
param_dist = {
    # Parámetros básicos
    'regressor__n_estimators': [50, 100, 200, 300, 500],
    'regressor__max_depth': [3, 4, 5, 6, 7, 9],
    'regressor__learning_rate': [0.01, 0.05, 0.1, 0.3, 0.5],
    
    # Regularización
    'regressor__reg_alpha': [0, 0.1, 0.5, 1, 5],  # L1
    'regressor__reg_lambda': [0, 0.1, 1, 5, 10],  # L2
    'regressor__min_child_weight': [1, 3, 5, 7],
    
    # Control de columnas/features
    'regressor__colsample_bytree': [0.3, 0.5, 0.7, 1.0],
    'regressor__colsample_bylevel': [0.3, 0.5, 0.7, 1.0],
    'regressor__subsample': [0.6, 0.8, 1.0],
    
    # Control de complejidad adicional
    'regressor__gamma': [0, 0.1, 0.2, 0.5],
    'regressor__max_leaves': [0, 3, 7, 15]
}

# Optimización
cv = KFold(n_splits=5, shuffle=True)#, random_state=42)
random_search = RandomizedSearchCV(
    model, param_dist, 
    n_iter=100,  # Aumentado debido a más parámetros
    cv=cv,
    scoring=['neg_root_mean_squared_error', 'neg_mean_absolute_error'],
    refit='neg_root_mean_squared_error',
    n_jobs=-1  # Usar todos los cores
)

# Entrenamiento y evaluación
random_search.fit(X_train, y_train)

# Resultados
best_params = random_search.best_params_
rmse = -random_search.best_score_
mae = -random_search.cv_results_['mean_test_neg_mean_absolute_error'][random_search.best_index_]

print(f'Best parameters: {best_params}')
print(f'RMSE: {rmse:.2f}')
print(f'MAE: {mae:.2f}')

# # Versión con más detalles
# feature_importance = pd.DataFrame({
#     'feature': train.columns,
#     'importance': random_search.best_estimator_.feature_importances_,
#     'importance_pct': random_search.best_estimator_.feature_importances_ / 
#                      random_search.best_estimator_.feature_importances_.sum() * 100
# }).sort_values('importance', ascending=False).round(4)


feature_importance = pd.DataFrame({
    'feature': [c for c in train if c != 'cost_of_living'],
    'importance': random_search.best_estimator_.named_steps['regressor'].feature_importances_,
    'importance_pct': random_search.best_estimator_.named_steps['regressor'].feature_importances_ / 
                     random_search.best_estimator_.named_steps['regressor'].feature_importances_.sum() * 100
}).sort_values('importance', ascending=False).round(4)