import requests
from prefect import flow, task, get_run_logger

base_url = "https://api.dados.rio/v2/adm_cor_comando"

headers = {"Accept-Language": "pt-BR,pt;q=0.5"}

@task(name="Get data from dados.rio API", retries=4, retry_delay_seconds=2)
def call_dadosrio_api(method: str, parameters: str = ''):
    response = requests.get(base_url+'/'+method+'/'+parameters, headers=headers)
    response.raise_for_status()
    response_json_dict = response.json()
    logger = get_run_logger()
    logger.debug(f"JSON Response: {response_json_dict}")
    return response_json_dict


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
            raise Exception("Empty events list: Nenhuma ocorrencia aberta reportada! This may not be an error per se, but we recommend to check whether the other systems are working as expected.")
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


@flow(name="ETL SMTRio pipeline")
def pipeline_ocorrencias_por_pop():
    # Make API Call of method that checks for ocorrências abertas
    api_ocorrencias_abertas_response = call_dadosrio_api('ocorrencias_abertas')
    list_of_events = transform_json_to_events_list(api_ocorrencias_abertas_response)
    set_of_pops = set()
    for event in list_of_events:
        set_of_pops.add(event['pop_id'])
    logger = get_run_logger()
    logger.info(f"Events with the following POPs were reported: {set_of_pops}") 
    # Make API call of method that returns all POPs and extract the POPs dictionary
    api_pops_response = call_dadosrio_api('pops')
    pops_dictionary = transform_json_to_pops_dictionary(api_pops_response)
    #new_pops_dict = {key: val for key in set_of_pops if (val := pops_dictionary.get(key))}
    pops_de_responsabilidade_do_CET_RIO = set()


if __name__ == "__main__":
    pipeline_ocorrencias_por_pop()
