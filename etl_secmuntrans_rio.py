import requests
import re
import csv
from prefect import flow, task, get_run_logger
import datetime
import pytz
from email.utils import parsedate_to_datetime
from collections import defaultdict

timezone_brasilia = pytz.timezone('America/Sao_Paulo')

base_url = "https://api.dados.rio/v2/adm_cor_comando"

sigla_orgao_de_interesse = "CET-RIO"

headers = {"Accept": "application/json",
           "Accept-Language": "pt-BR,pt;q=0.5"}

# Make API call of a given method with given parameters, and accept JSON as response
@task(name="Get data from dados.rio API", retries=4, retry_delay_seconds=2)
def call_dadosrio_api(method: str, parameters: str = ''):
    response = requests.get(base_url+'/'+method+'/'+parameters, headers=headers)
    response.raise_for_status()
    response_json_dict = response.json()
    logger = get_run_logger()
    logger.debug(f"Response header: {response.headers}")
    #api_datetime = datetime.datetime.strptime(response.headers['Date'], '%a, %d %b %Y %H:%M:%S %Z')
    api_datetime = parsedate_to_datetime(response.headers['Date'])
    logger.debug(f"Response datetime: {api_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.debug(f"JSON Response: {response_json_dict}")
    return api_datetime, response_json_dict


# This task transforms a JSON object into a list of events, if supplied a valid JSON object. Rudimentary consistency checks and error handling is implemented.
@task(name="Transform JSON into a list of events")
def transform_json_to_events_list(json_object):
    logger = get_run_logger()
    logger.debug(f"JSON Input keys: {json_object.keys()}") 
    logger.debug(f"JSON Input: {json_object}")
    try:
        events_list = json_object['eventos']
        logger.debug(f"Events list: {events_list}")
        if not events_list:
            logger.warning("Empty events list: Nenhuma ocorrencia reportada! This may not be an error per se, but we recommend to check whether the other systems are working as expected.")
        return events_list 
    except KeyError as error:
        logger.error(f"JSON object dictionary misses mandatory key:\n{error}")
        raise


# This task transforms a JSON object into a POPs dictionary, if supplied a valid JSON object. Rudimentary consistency checks and error handling is implemented.
@task(name="Transform JSON into a dictionary of POP pairs: id (as key) - name (as value)")
def transform_json_to_pops_dictionary(json_object):
    logger = get_run_logger()
    logger.debug(f"JSON Input keys: {json_object.keys()}") 
    logger.debug(f"JSON Input: {json_object}")
    try:
        # Check if key 'retorno' has value 'OK', if key does not exist KeyError is raised anyway
        if not json_object['retorno'] == 'OK':
            raise Exception("JSON object dictionary has key 'retorno' but its value is not equal to 'OK'.")
        pops_list = json_object['objeto']
        pops_dict = dict()
        for pop_entry in pops_list:
            pops_dict[pop_entry['id']] = pop_entry['titulo']
        logger.debug(f"Dictionary of POPs: {pops_dict}")
        if not pops_dict:
            raise Exception("Dictionary of POPs is empty, review dados.rio API documentation!")
        return pops_dict
    except KeyError as error:
        # Log the error message and raise the error again to prevent the task from continuing
        logger.error(f"JSON object dictionary misses mandatory key:\n{error}")
        raise


# This task transforms a JSON object into a list of activities, if supplied a valid JSON object. Rudimentary consistency checks and error handling is implemented.
@task(name="Transform JSON into a list of activities")
def transform_json_to_activities_list(json_object, pop_name):
    logger = get_run_logger()
    logger.debug(f"JSON Input keys: {json_object.keys()}") 
    logger.debug(f"JSON Input: {json_object}")
    try:
        # Check if the value of the key 'pop' coincides with the expected POP name , if key does not exist KeyError is raised anyway
        if not json_object['pop'] == pop_name:
            raise Exception("JSON object dictionary has key 'pop' but its value is not equal to the expected value: {pop_name}")
        activities_list = json_object['atividades']
        logger.debug(f"Activities list: {activities_list}")
        if not activities_list:
            logger.warning("Empty activities list: Nenhuma atividade a ser executada! This may not be an error per se, but it is at least unexpected result.")
        return activities_list
    except KeyError as error:
        logger.error(f"JSON object dictionary misses mandatory key:\n{error}")
        raise


@flow(name="ETL SMTRio pipeline")
def pipeline_ocorrencias_por_pop():
    # Make API call of method that checks for ocorrências abertas
    api_datetime_ocorrencias_abertas, api_ocorrencias_abertas_response = call_dadosrio_api('ocorrencias_abertas')
    list_of_ocorrencias_abertas = transform_json_to_events_list(api_ocorrencias_abertas_response)
    set_of_pops = set()
    for event in list_of_ocorrencias_abertas:
        set_of_pops.add(event['pop_id'])
    logger = get_run_logger()
    logger.info(f"Events with the following POPs were reported: {set_of_pops}") 
    # Make API call of method that checks for ocorrências fechadas
    next_request_datetime = api_datetime_ocorrencias_abertas - datetime.timedelta(days = 0)
    datetime_string_for_ocorrencias_fechadas_request = next_request_datetime.astimezone(timezone_brasilia).strftime('%Y-%m-%d 00:00:00.0')
    api_datetime_ocorrencias_fechadas, api_ocorrencias_fechadas_response = call_dadosrio_api('ocorrencias','?inicio='+datetime_string_for_ocorrencias_fechadas_request)
    list_of_ocorrencias_fechadas = transform_json_to_events_list(api_ocorrencias_fechadas_response)
    for event in list_of_ocorrencias_fechadas:
        set_of_pops.add(event['pop_id'])
    # Make API call of method that returns all POPs and extract the POPs dictionary
    api_datetime_pops, api_pops_response = call_dadosrio_api('pops')
    pops_dictionary = transform_json_to_pops_dictionary(api_pops_response)
    #new_pops_dict = {key: val for key in set_of_pops if (val := pops_dictionary.get(key))}
    set_of_pops_de_responsabilidade_do_orgao_de_interesse = set()
    for pop in set_of_pops:
        # Check if the POP is of responsability of CET-RIO
        try:
            api_datetime_pop_orgaos_responsaveis, api_pop_orgaos_responsaveis_response = call_dadosrio_api('procedimento_operacional_padrao_orgaos_responsaveis', f'?popId={pop}')
            activities_list = transform_json_to_activities_list(api_pop_orgaos_responsaveis_response, pops_dictionary[pop])
            for activity in activities_list:
                if activity['sigla'] == sigla_orgao_de_interesse:
                    set_of_pops_de_responsabilidade_do_orgao_de_interesse.add(pop)
                    break
        except BaseException as error:
            logger.error(f"An exception ocurred while checking responsability of POPs: {error}")
            logger.warning("Skipped pop_id: {pop}")
    logger.info(f"Events with the following POPs were reported: {set_of_pops}") 
    logger.info(f"The following POPs are relevant to CET-RIO: {set_of_pops_de_responsabilidade_do_orgao_de_interesse}")
    ocorrencias_por_categoria = defaultdict(int)
    for event in list_of_ocorrencias_abertas:
        logger.info(f"Event: {pops_dictionary[event['pop_id']]}, {event['status']}")
        identifier_string = pops_dictionary[event['pop_id']] + '_-_' + event['status']
        ocorrencias_por_categoria[identifier_string] += 1
    for event in list_of_ocorrencias_fechadas:
        identifier_string = pops_dictionary[event['pop_id']] + '_-_' + event['status']
        ocorrencias_por_categoria[identifier_string] += 1
        logger.info(f"Event: {pops_dictionary[event['pop_id']]}, {event['status']}")
    logger.info(f"Ocorrências por categoria: {ocorrencias_por_categoria}")
    datetime_string_for_ocorrencias_abertas = api_datetime_ocorrencias_abertas.astimezone(timezone_brasilia).strftime('%Y-%m-%d %H:%M:%S')
    datetime_string_for_ocorrencias_fechadas = api_datetime_ocorrencias_fechadas.astimezone(timezone_brasilia).strftime('%Y-%m-%d %H:%M:%S')
    with open('ocorrencias_por_categoria.csv', 'a') as f:
        csv_writer = csv.writer(f)
        if f.tell() == 0:
            csv_writer.writerow(['datetime', 'tipo_ocorrencia', 'status_ocorrencia', 'quantidade_ocorrencia'])
        for key in ocorrencias_por_categoria:
            composing_parts = re.split('_-_',key)
            status = composing_parts[-1]
            titulo = ''.join(composing_parts[:-1])
            datetime_string_for_entry = None
            if status == 'ABERTO':
                datetime_string_for_entry = datetime_string_for_ocorrencias_abertas
            else:
                datetime_string_for_entry = datetime_string_for_ocorrencias_fechadas
            # open the file in the write mode
            csv_writer.writerow([datetime_string_for_entry, titulo, status, ocorrencias_por_categoria[key]])
            logger.info(f"{datetime_string_for_entry}, {titulo}, {status}, {ocorrencias_por_categoria[key]}") 


if __name__ == "__main__":
    pipeline_ocorrencias_por_pop()
