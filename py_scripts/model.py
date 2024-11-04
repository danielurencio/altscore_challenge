import pandas as pd
import numpy as np
from sklearn.model_selection import KFold, cross_validate
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import RandomizedSearchCV

dataset = pd.read_csv('final_dataset.csv.gz')

# Definir columnas por tipo
categorical_cols = ['is_aream_capital_provincial', 'is_aream_cabecera_parroquial', 
                   'is_aream_localidad_amanzanada', 'is_aream_cabecera_cantonal']

numeric_cols = ['total_edificaciones', 'total_viviendas', 'avg_edificaciones', 
                'densidad_edificaciones', 'densidad_viviendas', 'area_total'] 

# Crear preprocessador
preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), numeric_cols),
        ('cat', OneHotEncoder(drop='first', sparse=False), categorical_cols)
    ])

# Crear pipeline
model = Pipeline([
    ('preprocessor', preprocessor),
    ('regressor', XGBRegressor())
])

# Parámetros para optimizar
param_dist = {
    'regressor__n_estimators': [100, 200, 300],
    'regressor__max_depth': [3, 4, 5, 6],
    'regressor__learning_rate': [0.01, 0.1, 0.3]
}

# Optimización con cross-validation
cv = KFold(n_splits=5, shuffle=True, random_state=42)
random_search = RandomizedSearchCV(
    model, param_dist, n_iter=10, cv=cv, 
    scoring=['neg_root_mean_squared_error', 'neg_mean_absolute_error'],
    refit='neg_root_mean_squared_error'
)

# Entrenar modelo
random_search.fit(X, y)

# Obtener mejores resultados
best_model = random_search.best_estimator_
rmse = -random_search.best_score_
mae = -random_search.cv_results_['mean_test_neg_mean_absolute_error'][random_search.best_index_]

print(f'Best parameters: {random_search.best_params_}')
print(f'RMSE: {rmse:.2f}')
print(f'MAE: {mae:.2f}')
