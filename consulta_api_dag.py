from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from subir_cs import upload_to_cloud_storage 
from subir_cs import get_most_relevant_items_for_category
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import bigquery


def subir_a_cloud_storage():
    # Configura las variables según sea necesario, como CATEGORY, BUCKET_NAME, FILE_NAME y DIRECTORY
    CATEGORY = "MLA438566"
    BUCKET_NAME = "southamerica-west1-proyecto-27cfb61e-bucket"
    FILE_NAME = "file.tsv"
    api_data = get_most_relevant_items_for_category(CATEGORY)
     # Campos que pueden contener "null" y deben corregirse
    fields_to_check = ["sold_quantity"]  # Agrega más campos aquí si es necesario

    # Corregir valores "null" a "NULL" en los campos especificados
    for item in api_data:
        for field in fields_to_check:
            if item[field] == "null":
                item[field] = 0

    # Llama a la función upload_to_cloud_storage para realizar el proceso de guardado en Cloud Storage
    upload_to_cloud_storage(api_data, BUCKET_NAME, FILE_NAME)

with DAG(
    dag_id="Extraer_Informacion_API",
    start_date=datetime(2023, 10, 21),
) as dag:
    task_inicio = DummyOperator(
        task_id="Inicio"
    )

    task_subir_a_cloud_storage = PythonOperator(
        task_id="Subir_a_Cloud_Storage",
        python_callable=subir_a_cloud_storage
    )



    # Define la información de la tarea GCSToBigQueryOperator
    task_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='Cargar_a_BigQuery',
        bucket='southamerica-west1-proyecto-27cfb61e-bucket',
        source_objects=['file.tsv'],
        destination_project_dataset_table='zinc-mantra-401718.datos_ml.consolas_ml_v4',
        source_format="CSV",
        field_delimiter="\t",
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        encoding="UTF-8",
        schema_object="esquema.json"
    )

    task_inicio >> task_subir_a_cloud_storage >> task_gcs_to_bigquery
