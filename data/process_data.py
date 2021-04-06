"""
PREPROCESSING DATA
Disaster Response Pipeline Project
Udacity - Data Science Nanodegree

Sample Script Execution:
> python process_data.py disaster_messages.csv disaster_categories.csv DisasterResponse.db

Arguments:
    1) CSV file containing messages (disaster_messages.csv)
    2) CSV file containing categories (disaster_categories.csv)
    3) SQLite destination database (DisasterResponse.db)
"""

import sys
import pandas as pd
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):
    '''
    INPUT:
        'message_filepath' : a string/path to a csv file
        'categories_filepath' : a string/path to a csv file
    OUTPUT:
        'df' : a loaded/transformed pandas dataframe
    '''

    messages = pd.read_csv(messages_filepath)  # load messages data
    categories = pd.read_csv(categories_filepath)  # load categories data

    # merge the datasets
    df = pd.merge(messages, categories, on='id')

    # split the categories into separate category columns
    categories = df['categories'].str.split(';', expand=True)

    # select the first row of the categories dataframe
    row = categories[:1]

    # extract numbers from columns
    category_colnames = row.apply(lambda x: x[0][:-2])

    # rename the columns of `categories`
    categories.columns = category_colnames

    for column in categories:
        # set each value to be the last character of the string
        categories[column] = categories[column].str.slice(-1)

        # convert column from string to numeric
        categories[column] = pd.to_numeric(categories[column])

    # drop the original categories column from `df`
    df = df.drop('categories', axis=1)

    # concatenate the original dataframe with the new `categories` dataframe
    df = pd.concat([df, categories], axis=1)

    return df


def clean_data(df):
    '''
    INPUT:
        - pandas dataframe
    OUTPUT:
        - pandas dataframe with duplicates removed
    '''
    # drop duplicates
    df = df.drop_duplicates()

    # remove 'original' column with high number of NaNs
    df = df.drop('original', axis=1)

    return df


def save_data(df, database_filename):
    '''
    INPUT:
        - cleaned dataframe ready to move to sql
        - string of desired database name, in format 'databasename.db'
    OUTPUT:
        - None
    '''

    # create sqlite engine
    engine = create_engine('sqlite:///' + str(database_filename))

    # send df to sqlite file, omitting the index
    df.to_sql('disaster_data', engine, index=False)


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)

        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)

        print('Cleaned data saved to database!')

    else:
        print('Please provide the filepaths of the messages and categories '
              'datasets as the first and second argument respectively, as '
              'well as the filepath of the database to save the cleaned data '
              'to as the third argument. Example: python process_data.py '
              'disaster_messages.csv disaster_categories.csv '
              'DisasterResponse.db')


if __name__ == '__main__':
    main()
