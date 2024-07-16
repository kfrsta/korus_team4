import pandas as pd
import re
import os
import logging


def get_pd_df(table, hook, scheme):
    query = f"SELECT * FROM {scheme}.{table}"
    df = hook.get_pandas_df(query)
    return df


def clean_column_names(column_name):
    column_name = column_name.lower()
    column_name = column_name.replace(' ', '_')
    column_name = ''.join(
        char for char in column_name if char.isalnum() or char == '_')
    return column_name


def clean_table_df(df: pd.DataFrame, drop_user_id:bool):
    df.drop_duplicates(inplace=True)
    if 'название' in df.columns:
        df['название'] = df['название'].str.replace(r'User:', '')
    df = df.map(lambda x: re.sub(
        r'\s*\[\d+\]', '', str(x)) if isinstance(x, str) else x)
    if 'Уровень знаний' in df.columns:
        df.dropna(subset=['Уровень знаний'], inplace=True)
    if all(col in df.columns for col in ['дата', 'Дата изм.']):
        df['дата'] = df['дата'].fillna(df['Дата изм.'])
    if drop_user_id:
        df.drop(columns=['активность', 'Сорт.', 'Дата изм.','название', 'User ID'], errors='ignore', inplace=True)
    else:
        df.drop(columns=['активность', 'Сорт.', 'Дата изм.','название'], errors='ignore', inplace=True)
    return df


def extract_numbers(text):
    if pd.isna(text):
        return text
    return '/'.join(re.findall(r'\[(\d+)\]', text))


def create_intermediate_table(df, column):
    intermediate_data = []
    for idx, row in df.iterrows():
        if pd.notna(row[column]):
            row[column] = extract_numbers(row[column])
            entries = row[column].split('/')
            for entry in entries:
                if entry:
                    resimeid = row['ResumeID']
                    intermediate_data.append([resimeid, entry])
    return pd.DataFrame(intermediate_data, columns=['resumeid', 'infoid'])
