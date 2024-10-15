import os


def write_dataframe_to_csv(dataframe, folder_name, file_name):
    path = os.path.join(folder_name, file_name)
    os.makedirs(folder_name, exist_ok=True)
    dataframe.to_csv(path, index=True)
