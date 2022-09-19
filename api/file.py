import pathlib
import pandas as pd


def fill_file_to_dataframe(file):

    file_name = file.filename

    extension = pathlib.Path(file_name).suffix
    print('extension')
    print(extension)

    if extension.lower() == '.csv':
        df = read_file_csv(file)
    elif extension.lower() == '.json':
        df= read_file_json(file)
    # elif extension.lower() == '.xml':
        # df = read_file_xml(file)
    else:
        return None

    return df


def read_file_csv(file):

    df = pd.read_csv(file)

    return df

def read_file_json(file):

    df = pd.read_json(file)

    return df

def read_file_xml(file):

    df = pd.read_xml(file,xpath="/row")

    return df    