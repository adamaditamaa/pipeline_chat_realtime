import os
import subprocess
from prefect.blocks.system import Secret
import tempfile
import yaml

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
                "--project-dir", f"./{path_folder}",
                "--profiles-dir", tmpdir
            ])

        result_exec = exec_dbt([
            "python", "-m", "dbt.cli.main", "run",
            "--project-dir", f"./{path_folder}",
            "--profiles-dir", tmpdir,
            *vars_args
        ])
        logger.info(result_exec)
    if result_exec != 0:
        raise ValueError(f"Error on DBT")
    return result_exec
        
