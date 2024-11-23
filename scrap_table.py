from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # Configura Selenium con el driver de Chrome
    service = Service('/path/to/chromedriver')  # Cambia esto por la ruta de tu ChromeDriver
    driver = webdriver.Chrome(service=service)

    try:
        # URL de la página web
        url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
        driver.get(url)

        # Esperar a que la tabla cargue (ajusta el tiempo si es necesario)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "table-responsive"))
        )

        # Obtener el HTML renderizado
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        # Buscar la tabla
        table = soup.find('table', {'class': 'table-responsive'})
        if not table:
            return {
                'statusCode': 404,
                'body': 'No se encontró la tabla en la página web'
            }

        # Extraer las filas de la tabla
        rows = []
        for row in table.find('tbody').find_all('tr'):
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

        # Guardar en DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('TablaWebScrapping')

        # Eliminar datos existentes
        scan = table.scan()
        with table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(Key={'id': each['id']})

        # Insertar los nuevos datos
        for row in rows:
            row['id'] = str(uuid.uuid4())
            table.put_item(Item=row)

        # Retornar los datos
        return {
            'statusCode': 200,
            'body': rows
        }

    finally:
        driver.quit()  # Cierra el navegador
