from prefect import flow, task,get_run_logger
from prefect.blocks.system import Secret

import os 
import sys
from pathlib import Path
import subprocess
import tempfile
import yaml
import warnings
warnings.filterwarnings("ignore")


def run_dbt(logger, path_folder, profiles=None, vars_dict=None,install_deps=None):

    def exec_dbt(args):
        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
            )

        # stream logs to Prefect
        for line in process.stdout:
            logger.info(line.strip())

        process.wait()
        return process.returncode

    if profiles:
        logger.info(f"Loading profiles from block: {profiles}")
        profiles = Secret.load(profiles).get()
    else:
        profiles = Secret.load("profile-dbt-postgres").get()

    logger.info(f"Using profiles: {profiles}")
        
    with tempfile.TemporaryDirectory() as tmpdir:
        profiles_path = os.path.join(tmpdir, "profiles.yml")
        with open(profiles_path, "w") as f:
            yaml.dump(profiles, f, sort_keys=False)

        vars_args = []
        if vars_dict:
            vars_str = yaml.dump(vars_dict, default_flow_style=True).strip()
            vars_args = ["--vars", vars_str]
            logger.info(f"Passing vars to dbt: {vars_str}")

        if install_deps:
            exec_dbt([
                "python", "-m", "dbt.cli.main", "deps",
                "--project-dir", f"{path_folder}",
                "--profiles-dir", tmpdir
            ])

        result_exec = exec_dbt([
            "python", "-m", "dbt.cli.main", "run",
            "--project-dir", f"{path_folder}",
            "--profiles-dir", tmpdir,
            *vars_args
        ])
        logger.info(result_exec)
    if result_exec != 0:
        raise ValueError(f"Error on DBT")
    return result_exec
        



##################################################################################

# Variable Path
cluster_name = "modeling_fact_leads_funnel"
current_script_path = os.path.abspath(__file__)
main_folder = os.path.dirname(current_script_path)
folder_main_modeling = 'Modeling'
folder_modeling = "modeling"

# Jika folder dbt kamu namanya 'modeling' dan ada di dalam folder yang sama dengan main.py:
DBT_PROJECT_PATH = folder_modeling
#DBT_PROJECT_PATH = os.path.join(main_folder,folder_main_modeling,cluster_name,folder_modeling)

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
    run_dbt(logger=logger,path_folder=DBT_PROJECT_PATH,install_deps=True)
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



