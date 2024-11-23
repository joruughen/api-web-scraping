import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web de los sismos
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    # Realizar la solicitud HTTP
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML
    soup = BeautifulSoup(response.content, 'html.parser')

    # Buscar la tabla con los sismos
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    # Extraer las filas de la tabla
    rows = []
    for row in table.find_all('tr')[1:]:  # Omitir el encabezado
        cells = row.find_all('td')
        if len(cells) > 0:
            rows.append({
                'Fecha': cells[0].text.strip(),
                'Hora': cells[1].text.strip(),
                'Latitud': cells[2].text.strip(),
                'Longitud': cells[3].text.strip(),
                'Profundidad': cells[4].text.strip(),
                'Magnitud': cells[5].text.strip(),
                'Ubicación': cells[6].text.strip()
            })

    # Limitar a los últimos 10 sismos
    rows = rows[:10]

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping')

    # Insertar los datos en la tabla
    for row in rows:
        row['id'] = str(uuid.uuid4())  # Generar un ID único
        table.put_item(Item=row)

    # Retornar los datos como resultado
    return {
        'statusCode': 200,
        'body': rows
    }
