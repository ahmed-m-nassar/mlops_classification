import os , sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(project_dir)

from database.manager import DatabaseManager
from encoding.encoding import MapTargetValues, AddPoutcomeFlag
from feature_engineering.feature_engineering import AddAgeFlag, SelectFeatures


import yaml
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
import logging
from sklearn.model_selection import GridSearchCV
import xgboost as xgb
from sklearn.metrics import confusion_matrix



logging.basicConfig(level=logging.INFO)



def read_params_file(file_path):
    """
    Reads a YAML parameters file and returns the content as a dictionary.

    Args:
        file_path (str): The path to the YAML parameters file.

    Returns:
        dict: A dictionary containing the parameters read from the file.
    """
    with open(file_path) as yaml_file:
        params = yaml.safe_load(yaml_file)
    return params


def connect_to_database(params):
    """
    Connects to the database using the provided parameters.

    Args:
        params (dict): Database connection parameters.

    Returns:
        DatabaseManager: An instance of DatabaseManager representing the database connection.
    """
    db_manager = DatabaseManager(dbname=params['database']['config']['dbname'],
                                 host=params['database']['config']['host'],
                                 user=params['database']['config']['user'],
                                 port=params['database']['config']['port'],
                                 password=params['database']['config']['password'])
    return db_manager


def create_pipeline():
    """
    Creates a pipeline for data processing.

    Returns:
        Pipeline: The pipeline object.
    """
    map_target = MapTargetValues()
    add_poutcome_flag = AddPoutcomeFlag()
    add_age_flag = AddAgeFlag()
    select_features = SelectFeatures()

    pipeline = Pipeline([
        ('map_target', map_target),
        ('add_poutcome_flag', add_poutcome_flag),
        ('add_age_flag', add_age_flag),
        ('select_features', select_features)
    ])

    return pipeline


def train_model(train_df_transformed):
    """
    Trains an XGBoost classifier using grid search.

    Args:
        train_df_transformed (pd.DataFrame): The transformed training dataset.
        params (dict): Parameters for grid search.

    Returns:
        xgb.XGBClassifier: The best trained XGBoost classifier.
    """
    # Calculate the number of samples in each class
    num_positive_samples = len(train_df_transformed[train_df_transformed['"y"'] == 1])
    num_negative_samples = len(train_df_transformed[train_df_transformed['"y"'] == 0])

    # Calculate the ratio of negative to positive samples
    scale_pos_weight = num_negative_samples / num_positive_samples

    # Define the parameter grid for grid search
    param_grid = {
        'n_estimators': [100, 200, 300],  # Number of trees
        'max_depth': [3, 5, 7],  # Maximum depth of each tree
        'learning_rate': [0.1, 0.01, 0.001],  # Learning rate
        'scale_pos_weight': [scale_pos_weight]  # Scale pos weight for imbalanced class
    }

    # Define the XGBoost classifier
    xgb_classifier = xgb.XGBClassifier()

    # Split the features and target variable
    X_train = train_df_transformed.drop(['"y"'], axis=1)
    y_train = train_df_transformed['"y"']

    # Perform grid search
    grid_search = GridSearchCV(estimator=xgb_classifier, param_grid=param_grid, cv=5)
    grid_search.fit(X_train, y_train)

    # Get the best model from the grid search
    best_xgb_model = grid_search.best_estimator_

    return best_xgb_model


def evaluate_model(model, val_df_transformed):
    """
    Evaluates the trained model on the validation dataset.

    Args:
        model (xgb.XGBClassifier): The trained XGBoost classifier.
        val_df_transformed (pd.DataFrame): The transformed validation dataset.
    """
    # Split the features and target variable
    X_val = val_df_transformed.drop(['"y"'], axis=1)
    y_val = val_df_transformed['"y"']

    # Make predictions on the validation dataset
    val_predictions = model.predict(X_val)

    # Compute the confusion matrix
    cm = confusion_matrix(y_val, val_predictions)

    # Print the confusion matrix
    print("Confusion Matrix:")
    print(cm)


if __name__ == "__main__":
    params = read_params_file(os.path.join('config', 'params.yaml'))

    # Connect to the database
    db_manager = connect_to_database(params)
    train_table_name = params['database']['train_table_name']
    df = db_manager.select_from_table(table_name=train_table_name,
                                      schema_file_path=params['schemas']['training_schema_path'])

    # Split the dataset into train and validation sets
    train_df, val_df = train_test_split(df, test_size=params['split_data']['train_test_split'],
                                        random_state=params['base']['random_state'], stratify=df['"y"'])

    # Create the pipeline
    pipeline = create_pipeline()

    # Apply the pipeline to the training dataset
    train_df_transformed = pipeline.transform(train_df)

    # Train the model using grid search
    best_xgb_model = train_model(train_df_transformed)

    # Apply the pipeline to the validation dataset
    val_df_transformed = pipeline.transform(val_df)

    # Evaluate the model on the validation dataset
    evaluate_model(best_xgb_model, val_df_transformed)