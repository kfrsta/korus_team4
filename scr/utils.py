import re

def clean_column_names(column_name: str) -> str:
    column_name = column_name.lower()
    replacements = {
        ' ': '_',
        '-': '',
        '/': '',
        ', ': '_',
        '.': ''
    }
    for old, new in replacements.items():
        column_name = column_name.replace(old, new)
    column_name = ''.join(char for char in column_name if char.isalnum() or char == '_')
    return column_name
