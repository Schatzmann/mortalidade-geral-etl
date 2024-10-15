def convert_date_structure(date):
    if date is not None:
        return date[4:8] + "-" + date[2:4] + "-" + date[0:2]

def convert_time_structure(time):
    if time is not None:
        return time[0:2] + ":" + time[2:4] + ":00"
    
def convert_age(age:str):

    if age is None:
        return None, None
    
    unidade_medida_idade = age[0]

    if unidade_medida_idade == '0':
        return unidade_medida_idade, f'00:00:{age[1:]}'
    elif unidade_medida_idade == '1':
        return unidade_medida_idade, f'00:{age[1:]}:00'
    elif unidade_medida_idade == '2':
        return unidade_medida_idade, f'{age[1:]}:00:00'
    elif unidade_medida_idade in ['3', '4', '5']:
        return unidade_medida_idade, age[1:]
    elif unidade_medida_idade == '9':
        return unidade_medida_idade, None


def remove_columns(dataframe, list_columns_to_keep):
    return dataframe[list_columns_to_keep]

def extract_occurrence_state(municipality_code):
    if municipality_code is None:
        return None
    
    return municipality_code[0:2]

def convert_in_gender_char(sex):
    if sex is None:
        return None
    elif sex == '1':
        return 'M'
    elif sex == '2':
        return 'F'
    elif sex == '0':
        return 'I'