import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página de los sismos reportados
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    # Realizar la solicitud HTTP a la página
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML
    soup = BeautifulSoup(response.content, 'html.parser')

    # Buscar la tabla con los datos de los sismos
    table = soup.find('table', {'class': 'table-responsive'})
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    # Extraer las filas de la tabla
    rows = []
    for row in table.find('tbody').find_all('tr'):  # Navegar dentro del tbody
        cells = row.find_all('td')
        if len(cells) > 0:
            rows.append({
                'reporte': cells[0].text.strip(),
                'referencia': cells[1].text.strip(),
                'fecha_hora': cells[2].text.strip(),
                'magnitud': cells[3].text.strip(),
                'descarga': cells[4].text.strip()
            })

    # Limitar los resultados a los últimos 10 sismos
    rows = rows[:10]

    # Conectar con DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping')  # Asegúrate de tener esta tabla creada

    # Eliminar todos los elementos existentes en la tabla
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'id': each['id']
                }
            )

    # Insertar los nuevos datos en la tabla
    for row in rows:
        row['id'] = str(uuid.uuid4())  # Generar un ID único
        table.put_item(Item=row)

    # Retornar los datos como respuesta
    return {
        'statusCode': 200,
        'body': rows
    }
