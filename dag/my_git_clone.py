import os
import base64
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

def get_sa_json_from_conn(conn_id="google_cloud_default") -> str:
    """
    Returns the service account JSON as a compact string from an Airflow Connection.
    UI field "Keyfile JSON" is stored in extras as keyfile_dict (dict).
    """
    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson or {}
    # Common key used by the Google provider:
    keyfile = extras.get("keyfile_dict")
    if keyfile is None:
        # Fallbacks if someone stored it under another key as a JSON string
        for k in ("keyfile_json", "credentials", "keyfile"):
            if k in extras:
                val = extras[k]
                keyfile = json.loads(val) if isinstance(val, str) else val
                break
    if not keyfile:
        raise ValueError(f"No Keyfile JSON found in connection '{conn_id}'")
    return json.dumps(keyfile, separators=(",", ":"))  # compact, safe for env


def get_sa_json_b64(conn_id="google_cloud_default") -> str:
    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson or {}
    keyfile = extras.get("keyfile_dict")

    if keyfile is None:
        raise ValueError(f"No keyfile_dict in connection {conn_id}")

    # Handle both dict and already-JSON-string
    if isinstance(keyfile, dict):
        raw_json = json.dumps(keyfile, separators=(",", ":"))
    elif isinstance(keyfile, str):
        # If it’s JSON string, use as-is; if not JSON, raise
        try:
            parsed = json.loads(keyfile)
        except Exception as e:
            raise ValueError("keyfile_dict is a non-JSON string") from e
        raw_json = json.dumps(parsed, separators=(",", ":"))
    else:
        raise ValueError("keyfile_dict must be dict or JSON string")

    return base64.b64encode(raw_json.encode()).decode()

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

    dbt_check = BashOperator(
        task_id="dbt_check",
        bash_command=f"""
        dbt --version
        """,
    )

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


    dbt_run_2 = BashOperator(
        task_id="dbt_run_2",
        bash_command=r"""
    set -euo pipefail
    TMP_DIR="$(mktemp -d)"; umask 077
    printf '%s' "$GOOGLE_CREDENTIALS_JSON" > "$TMP_DIR/sa.json"

    cat > "$TMP_DIR/profiles.yml" <<'YAML'
    my_dbt_project:
    target: prod
    outputs:
        prod:
        type: bigquery
        method: service-account
        keyfile: __KEYFILE__
        project: ${GCP_PROJECT}
        dataset: ${BQ_DATASET}
        location: us-central1
    YAML
    sed -i "s#__KEYFILE__#${TMP_DIR}/sa.json#g" "$TMP_DIR/profiles.yml"

    DBT_PROFILES_DIR="$TMP_DIR" dbt run \
    --project-dir /usr/local/airflow/dags/dbt_project \
    --target-dir /tmp/dbt_target

    rm -rf "$TMP_DIR"
    """,
        env={
            "GOOGLE_CREDENTIALS_JSON": "{{ conn.google_cloud_default.extra_dejson['keyfile_dict'] | tojson }}",
            "GCP_PROJECT": "{{ conn.google_cloud_default.extra_dejson.get('project', '') }}",
            "BQ_DATASET": "analytics",
        },
    )



        # Get the Keyfile JSON as a string (kept out of logs)
    gcp_sa_b64  = get_sa_json_b64("google_cloud_default")

    write_keyfile = BashOperator(
        task_id="write_keyfile_json",
        # Use printf (not plain echo) to preserve content exactly; set tight perms.
        bash_command=r"""
            set -euo pipefail
            umask 077
            target="${TARGET:-/dev/shm/gcp-sa.json}"

            # Use printf (not echo) and decode base64
            printf '%s' "$GCP_SA_JSON_B64" | base64 -d > "$target"

            echo "Service account JSON written to $target"
        """,
        env={
             "GCP_SA_JSON_B64": gcp_sa_b64,
            "TARGET": "/dev/shm/gcp-sa.json",    # change path if you like
        },
        do_xcom_push=False,
    )

    write_keyfile_and_profiles = BashOperator(
        task_id="write_keyfile_and_profiles",
        bash_command=r"""
set -euo pipefail
umask 077

KEYFILE_PATH="${KEYFILE_PATH:-/dev/shm/gcp-sa.json}"
PROFILES_DIR="${DBT_PROFILES_DIR:-/dev/shm/dbt}"
mkdir -p "$PROFILES_DIR"

# 1) Decode service account JSON
printf '%s' "$GCP_SA_JSON_B64" | base64 -d > "$KEYFILE_PATH"

# 2) Write dbt profiles.yml
cat > "${PROFILES_DIR}/profiles.yml" <<-EOF
my_bq_project:
target: prod
outputs:
    prod:
    type: bigquery
    method: service-account
    keyfile: ${KEYFILE_PATH}
    project: ${PROJECT_ID}
    dataset: ${BQ_DATASET}
    location: ${BQ_LOCATION}
    threads: ${DBT_THREADS}
    priority: ${DBT_PRIORITY}
    job_retries: ${DBT_JOB_RETRIES}
EOF

echo "Wrote $KEYFILE_PATH and ${PROFILES_DIR}/profiles.yml"
        """,
        env={
            "GCP_SA_JSON_B64": gcp_sa_b64,
            "KEYFILE_PATH": "/dev/shm/gcp-sa.json",
            "DBT_PROFILES_DIR": "/dev/shm/dbt",
            "PROJECT_ID": "your_project_id",
            "BQ_DATASET": "your_dataset",    # customize
            "BQ_LOCATION": "US",             # e.g. "US" or "EU"
            "DBT_THREADS": "4",
            "DBT_PRIORITY": "interactive",
            "DBT_JOB_RETRIES": "1",
        },
        do_xcom_push=False,
    )
  

    #clone_repo >> dbt_compile >> dbt_run >> dbt_generate_documentation
    write_keyfile >> write_keyfile_and_profiles