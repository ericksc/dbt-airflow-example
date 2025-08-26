import os
import json
import yaml
import tempfile
import subprocess
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


def get_gcp_credentials_dict(gcp_conn_id="google_cloud_default"):
    hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
    conn = hook.get_connection(gcp_conn_id)

    keyfile_dict = conn.extra_dejson.get("keyfile_dict")

    if not keyfile_dict:
        raise ValueError(f"No keyfile_dict found in connection {gcp_conn_id}")

    # If it’s a string, parse it into dict
    if isinstance(keyfile_dict, str):
        keyfile_dict = json.loads(keyfile_dict)

    return keyfile_dict



def run_dbt_task(command: str, bq_dataset: str = "my_dataset", bq_project: str = "my_project", dbt_project: str = "my_dbt_project"):
    """Generic runner for dbt commands with RAM-only profiles.yml"""
    # Create a RAM-backed temp folder
    with tempfile.TemporaryDirectory(dir="/dev/shm") as tmpdir:
        # 1. Load GCP creds from Airflow connection
        creds_dict = get_gcp_credentials_dict(gcp_conn_id="google_cloud_default")

        # 2. Build profiles.yml in memory
        profile = {
            dbt_project: {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "dataset": bq_dataset,
                        "job_execution_timeout_seconds": 300,
                        "job_retries": 1,
                        "keyfile_json": creds_dict,
                        "location": "US",
                        "method": "service-account-json",
                        "priority": "interactive",
                        "type": "bigquery",
                        "project": bq_project,
                        "threads": 1
                    }
                }
            }
        }

        print(profile)
        profile_path = os.path.join(tmpdir, "profiles.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile, f)

        # 3. Run dbt with DBT_PROFILES_DIR pointing to tmpdir
        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = tmpdir

        result = subprocess.run(
            ["dbt"] + command.split(),
            env=env,
            capture_output=True,   # capture stdout & stderr
            text=True,             # decode bytes to str
        )

        print(result.stdout)

        if result.returncode != 0:
            raise RuntimeError(f"dbt {command} failed:\n{result.stderr}")


        # 4. tmpdir (and profiles.yml) is auto-deleted here



REPO_NAME = "dbt-airflow-example"

# Get connection safely
conn = BaseHook.get_connection("github_pat")
GITHUB_USER = conn.login
GITHUB_PAT = conn.password

GIT_URL = f"https://{GITHUB_USER}:{GITHUB_PAT}@github.com/{GITHUB_USER}/{REPO_NAME}.git"

default_args = {"owner": "airflow"}

with DAG(
    "git_clone_with_pat",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    dbt_project_name = "my_dbt_project"
    dbt_repo_path = f"/tmp/{REPO_NAME}"
    dbt_project_path = f"{dbt_repo_path}/{dbt_project_name}"


    clean_up_1 = BashOperator(
        task_id="clean_up_1",
        bash_command=f"""
        rm -rf {dbt_repo_path}
        """,
    )

    clean_up_2 = BashOperator(
        task_id="clean_up_2",
        bash_command=f"""
        rm -rf {dbt_repo_path}
        """,
    )

    clone_repo = BashOperator(
        task_id="clone_private_repo",
        bash_command=f"""
        rm -rf {dbt_repo_path} && \
        git clone {GIT_URL} {dbt_repo_path}
        """,
    )

    dbt_debug = PythonOperator(
        task_id="dbt_debug",
        python_callable=run_dbt_task,
        op_args=[f"debug --project-dir {dbt_project_path}", "analytics", "dbt-poc-469617", "my_dbt_project"],   # dbt run
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt_task,
        op_args=[f"run --project-dir {dbt_project_path}", "analytics", "dbt-poc-469617", "my_dbt_project"],   # dbt run
    )


    dbt_generate_documentation = PythonOperator(
        task_id="dbt_doc",
        python_callable=run_dbt_task,
        op_args=[f"docs generate --project-dir {dbt_project_path} --static", "analytics", "dbt-poc-469617", "my_dbt_project"],   # dbt run
    )


    push_static_index = BashOperator(
        task_id="push_static_index",
        bash_command=f"""
        cd {dbt_project_path} &&
        git fetch origin dbt_docs &&
        git checkout dbt_docs &&

        cd {dbt_project_path}/target &&

        git config user.name "ericksc" &&
        git config user.email "ericksc@gmail.com" &&

        git add -f static_index.html &&
        git status &&
        git commit -m "Update static_index.html from Airflow run."
        git push origin dbt_docs
        """,
    )

    # dbt_compile = BashOperator(
    #     task_id="dbt_compile",
    #     bash_command=f"""
    #     dbt compile  --project-dir {dbt_project_path} --profiles-dir {dbt_repo_path}
    #     """,
    #     env={
    #     "DBT_GOOGLE_KEYFILE_JSON": "{{ json.dumps(creds) }}"  # Airflow Variable
    # }
    # )    

    # dbt_run = BashOperator(
    #     task_id="dbt_run",
    #     bash_command=f"""
    #     dbt run --project-dir {dbt_project_path} --profiles-dir {dbt_repo_path}
    #     """,
    #     env={
    #     "DBT_GOOGLE_KEYFILE_JSON": "{{ json.dumps(creds) }}"  # Airflow Variable
    # }
    # )


    # dbt_generate_documentation = BashOperator(
    #     task_id="dbt_doc",
    #     bash_command=f"""
    #     dbt docs generate --project-dir {dbt_project_path} --profiles-dir {dbt_repo_path} --static
    #     """,
    #     trigger_rule=TriggerRule.ONE_SUCCESS,
    #     env={
    #     "DBT_GOOGLE_KEYFILE_JSON": "{{ json.dumps(creds)}}"  # Airflow Variable
    # }
    # )    

    #clone_repo >> dbt_compile >> dbt_run >> dbt_generate_documentation
    clean_up_1 >> clone_repo >>  dbt_debug >> dbt_run >> dbt_generate_documentation >> push_static_index # >> clean_up_2