# script para consultar la API de Mercado Libre y guardar la información relevante en un archivo TSV
import requests
import json
from datetime import date
from google.cloud import storage
from google.cloud.storage import Blob

# Variable para obtener la fecha, sin separadores
DATE = str(date.today()).replace('-', '')

def get_most_relevant_items_for_category(category):
    """
    Recibe los ítems más relevantes por categoría desde la API.
    """
    url = f'https://api.mercadolibre.com/sites/MLA/search?category={category}#json'
    response = requests.get(url).text
    response = json.loads(response)
    data = response.get("results", [])  
    relevant_data = []
    for item in data:
        relevant_item = {
            'id': getKeyFromItem(item, 'id'),
            'site_id': getKeyFromItem(item, 'site_id'),
            'title': getKeyFromItem(item, 'title'),
            'price': getKeyFromItem(item, 'price'),
            'sold_quantity': getKeyFromItem(item, 'sold_quantity'),
            'condition': getKeyFromItem(item, 'condition'),
            'thumbnail': getKeyFromItem(item, 'thumbnail')
        }
        relevant_data.append(relevant_item)
    return relevant_data

def upload_to_cloud_storage(data, bucket_name, file_name):
    """
    Sube los datos a Cloud Storage.
    """
    # Inicializamos el cliente de Cloud Storage
    client = storage.Client()

    # Obtenemos el bucket de Cloud Storage
    bucket = client.get_bucket(bucket_name)

    # Creamos un objeto de tipo Blob con el nombre deseado dentro del bucket
    blob = Blob(file_name, bucket)

    # Lista para almacenar los datos a escribir
    lines = []

    for item in data:
        lines.append(f"{item['id']}\t{item['site_id']}\t{item['title']}\t{item['price']}\t{item['sold_quantity']}\t{item['condition']}\t{item['thumbnail']}\t{DATE}")

    # Imprimir el contenido de lines
    print(lines)

    # Subimos el archivo al blob con el content type
    blob.upload_from_string('\n'.join(lines).encode('utf-8'), content_type='text/plain; charset=utf-8')

    # Configuración de permisos para UBLA
    blob.acl.save_predefined("projectPrivate")

    # Imprimimos la URL pública del blob
    print(blob.public_url)

def getKeyFromItem(item, key):
    """
    Recibe la key que necesitamos sacar del diccionario almacenado en item.
    """
    return str(item[key]).replace('','').strip() if item.get(key) else "null"

def main():
    CATEGORY = "MLA438566"  # Reemplaza esta categoría por la que desees consultar
    BUCKET_NAME = "pruebasapiexp"
    FILE_NAME = "file.tsv"

    # Obtener datos de la API
    api_data = get_most_relevant_items_for_category(CATEGORY)
     # Campos que pueden contener "null" y deben corregirse
    fields_to_check = ["sold_quantity"]  # Agrega más campos aquí si es necesario

    # Corregir valores "null" a "NULL" en los campos especificados
    for item in api_data:
        for field in fields_to_check:
            if item[field] == "null":
                item[field] = "NULL"

    # Subir datos a Cloud Storage
    upload_to_cloud_storage(api_data, BUCKET_NAME, FILE_NAME)

if __name__ == "__main__":
    main()
