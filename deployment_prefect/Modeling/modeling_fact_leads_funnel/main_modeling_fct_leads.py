from prefect import flow, task,get_run_logger
from prefect.blocks.notifications import DiscordWebhook


import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from modules import dbt
import warnings
import logging
warnings.filterwarnings("ignore")

##################################################################################

# Variable Path
cluster_name = "modeling_fact_leads_funnel"
current_script_path = os.path.abspath(__file__)
main_folder = os.path.dirname(current_script_path)
folder_main_modeling = 'Modeling'
folder_modeling = "modeling"
DBT_PROJECT_PATH = os.path.join(folder_main_modeling,cluster_name,folder_modeling)


###############################################################################################################################################################

@task(name='Transform Transactions ISP',
      log_prints=True,
      cache_result_in_memory=False,
      retries=2,
      retry_delay_seconds=10)
def transform_transactions_isp(logger):
    dbt.run_dbt(logger=logger,path_folder=DBT_PROJECT_PATH)
    return 'Success Transform'
    
@flow(log_prints=True)
def modeling():
    logger = get_run_logger()
    try:
        transform_transactions_isp(logger)
    except Exception as error:
        raise error

if __name__ == "__main__":
    modeling()



