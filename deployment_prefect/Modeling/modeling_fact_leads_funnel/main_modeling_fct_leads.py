from prefect import flow, task,get_run_logger

import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from modules import dbt
import warnings
warnings.filterwarnings("ignore")

##################################################################################

# Variable Path
cluster_name = "modeling_fact_leads_funnel"
current_script_path = os.path.abspath(__file__)
main_folder = os.path.dirname(current_script_path)
folder_main_modeling = 'Modeling'
folder_modeling = "modeling"
DBT_PROJECT_PATH = os.path.join(folder_main_modeling,cluster_name,folder_modeling)

# Prefect Config
retry_count = 1
retry_delay = 10 # in seconds

###############################################################################################################################################################

@task(name='Transform Fact Leads Funnel',
      log_prints=True,
      cache_result_in_memory=False,
      retries=retry_count,
      retry_delay_seconds=retry_delay)
def transform_(logger):
    dbt.run_dbt(logger=logger,path_folder=DBT_PROJECT_PATH)
    return 'Success Transform'
    
@flow(log_prints=True)
def modeling():
    logger = get_run_logger()
    try:
        transform_(logger)
    except Exception as error:
        raise error

if __name__ == "__main__":
    modeling()



