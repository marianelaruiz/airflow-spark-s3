# Criar ambiente virtual/ativar
python3 -m venv airflow_venv
source airflow_venv/bin/activate

# Install requirements
# $ pip install -r requirements.txt

# Donde va a funcionar airflow
# export AIRFLOW_HOME="/home/marianela/Documentos/BulkConsulting/Cursos/3-Engenharia-de-dados/3-Apache-Airflow/lakehouse-test/lakehouse-airflow"

cd airflow
export AIRFLOW_HOME=$(pwd)

export AIRFLOW_VERSION=2.9.0
export PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow>=2.9.0" --constraint "${CONSTRAINT_URL}"

# Iniciar Airflow
airflow db init

# Criar un ausuario administrador
airflow users create     --username admin     --firstname marianela     --lastname ruiz     --email marianelaruiz.br@gmail.com     --role Admin     --password admin

airflow standalone

# Iniciar os serviços do Airflow
export AIRFLOW_HOME="/home/marianela/Documentos/BulkConsulting/Cursos/3-Engenharia-de-dados/3-Apache-Airflow/lakehouse-airflow/"
airflow webserver --port 8080
airflow scheduler


# segue um exemplo de caminho dinâmico que funciona em qualquer máquina para usar nos scripts da atividade:
base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
