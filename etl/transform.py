def convert_date_structure(date):
    return date[4:8] + "-" + date[2:4] + "-" + date[0:2]

def convert_time_structure(time):
    return time[0:2] + ":" + time[2:4] + ":00"

def remove_columns(dataframe, list_columns_to_keep):
    return dataframe[list_columns_to_keep]