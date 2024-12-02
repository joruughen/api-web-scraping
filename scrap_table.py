import boto3
import uuid

def lambda_handler(event, context):
    # Configuración
    dynamodb = boto3.resource('dynamodb')
    table_name = "TablaWebScrapping"

    # Intentar crear la tabla si no existe
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',  # Clave primaria
                    'KeyType': 'HASH'      # Partition Key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'  # Tipo String
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        # Esperar a que la tabla esté activa
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        # La tabla ya existe
        pass

    # Conectar a la tabla DynamoDB
    table = dynamodb.Table(table_name)

    # Datos de ejemplo para insertar
    data = [
        {
            "fecha_hora": "16/11/2024 10:00",
            "magnitud": "3.8",
            "ubicacion": "39 km al S de Lomas, Caravelí - Arequipa"
        },
        {
            "fecha_hora": "16/11/2024 14:00",
            "magnitud": "4.0",
            "ubicacion": "13 km al S de Tibillo, Palpa - Ica"
        },
        {
            "fecha_hora": "13/11/2024 08:30",
            "magnitud": "3.5",
            "ubicacion": "12 km al NE de Pozuzo, Oxapampa - Pasco"
        },
        {
            "fecha_hora": "14/11/2024 18:20",
            "magnitud": "3.5",
            "ubicacion": "23 km al SO de Chilca, Cañete - Lima"
        },
        {
            "fecha_hora": "15/11/2024 12:10",
            "magnitud": "3.8",
            "ubicacion": "20 km al O de Atico, Caravelí - Arequipa"
        },
        {
            "fecha_hora": "15/11/2024 17:45",
            "magnitud": "4.2",
            "ubicacion": "34 km al S de Pisco, Pisco - Ica"
        },
        {
            "fecha_hora": "14/11/2024 09:40",
            "magnitud": "4.1",
            "ubicacion": "51 km al O de Curimaná, Padre Abad - Ucayali"
        },
        {
            "fecha_hora": "14/11/2024 15:30",
            "magnitud": "3.5",
            "ubicacion": "23 km al SO de Ancón, Lima - Lima"
        },
        {
            "fecha_hora": "18/11/2024 02:15",
            "magnitud": "4.8",
            "ubicacion": "19 km al S de Camaná, Camaná - Arequipa"
        },
        {
            "fecha_hora": "13/11/2024 04:20",
            "magnitud": "3.6",
            "ubicacion": "8 km al O de Aucayacu, Leoncio Prado - Huánuco"
        }
    ]

    # Insertar datos en DynamoDB
    for item in data:
        item['id'] = str(uuid.uuid4())  # Generar un ID único para cada elemento
        table.put_item(Item=item)

    # Respuesta de éxito
    return {
        "statusCode": 200,
        "body": "Datos subidos exitosamente a DynamoDB"
    }
