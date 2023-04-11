import json
import boto3
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key, Attr
import decimal
import time
from LockClientED import LockClientED


def lambda_handler(event, context):
    
    ddb = boto3.client('dynamodb')


    # Obtener usuario de los parámetros del evento
    user_id = event.get('user_id')
    job_id = event.get('job_id')
    
    # Obtener fecha de hoy
    today = datetime.today().strftime('%Y-%m-%d')
    print(today)
    

    table = boto3.resource('dynamodb').Table('ApplicationsTable')
    
    
    #nueva version:
    responseJob = table.query(
        KeyConditionExpression=Key('user_id_job_id').eq(job_id),
        ProjectionExpression='user_id_job_id, date_c, count_applications, lock_key, expiration_time',
        Limit=1,
        ScanIndexForward=False
    )
    itemsJob = responseJob.get('Items', [])
    if not itemsJob:
        return {
            'statusCode': 404,
            'body': 'Record Job Applications not found'
        }
    itemJob = itemsJob[0]
    print("itemJob: ", itemJob)
    
    if 'count_applications' in itemJob and itemJob['count_applications'] < 1:
        return {
            'statusCode': 400,
            'body': 'User cannot apply to more jobs today'
        }
    elif 'count_applications' not in itemJob:
        return {
            'statusCode': 400,
            'body': 'Key "count_applications" not found in job item'
        }
    
    # Realizamos la búsqueda en la tabla
    responseUser = table.query(
        KeyConditionExpression=Key('user_id_job_id').eq(user_id) & Key('date_c').eq(today)
    )
    itemsUser = responseUser.get('Items', [])
    if itemsUser:
        itemUser = itemsUser[0]
    
    # Si no se encontró ningún registro, creamos uno nuevo
    if not itemsUser:
        new_itemUser = {
            'user_id_job_id': user_id,
            'date_c': today,
            'count_applications': 10
        }
        table.put_item(Item=new_itemUser)
        itemsUser = [new_itemUser]

    elif 'count_applications' in itemUser and itemUser['count_applications'] < 1:
        return {
            'statusCode': 400,
            'body': 'User cannot apply to more jobs today'
        }
    elif 'count_applications' not in itemUser:
        return {
            'statusCode': 400,
            'body': 'Key "count_applications" not found in user_id item'
        }
    itemUser = itemsUser[0]

    items = [itemUser, itemJob]



    # Crear la lista de claves de partición
    partition_keys = [(f'{user_id}', today), (f'{job_id }', itemJob['date_c'])]
    print(partition_keys)
    
    
    # # Versión vieja
    # #realizar la consulta previa
    # items = []
    # count_values = []
    # for partition_key in partition_keys:
    #     response = table.query(
    #         KeyConditionExpression='user_id_job_id = :user_id_job_id AND #date_c = :date_c',
    #         ExpressionAttributeNames={'#date_c': 'date_c'},
    #         ExpressionAttributeValues={':user_id_job_id': partition_key[0], ':date_c': partition_key[1]}
    #     )
    #     items.extend(response.get('Items', []))
    #     
    #     for item in response.get('Items', []):
    #         if 'count_applications' in item and item['count_applications'] < 1:
    #             if partition_key[0] == user_id:
    #                 return {
    #                     'statusCode': 400,
    #                     'body': 'User cannot apply to more jobs today'
    #                 }
    #             else:
    #                 return {
    #                     'statusCode': 400,  
    #                     'body': 'Job cannot receive more applications'
    #                 }
    
    
        
        
    # print("revisar que traigan los datos: ", items)

    # # Check if any items were returned
    # if not items:
    #     return {
    #         'statusCode': 404,
    #         'body': 'Record not found'
    #     }

    # Check if the record is locked due to expiration
    for item in items:
        lock_key = item.get('lock_key')
        if lock_key is not None:
            # lock_key exists, continue with expiration_time validation
            expiration_time = item.get('expiration_time')
            if expiration_time is not None:
                now = datetime.now()
                stored_dt = datetime.fromtimestamp(int(expiration_time))
                if now < stored_dt:
                    # Record is locked
                    return {
                        'statusCode': 423,
                        'body': 'Record is locked'
                    }
        else:
            # lock_key does not exist, skip this item
            continue
    
    

            
    # Obtener el bloqueo y hacer la consulta y actualización
    lock_client = LockClientED(dynamo_client=ddb, table_name='ApplicationsTable')
    
    with lock_client:
        locks = lock_client.acquire_locks(partition_keys, items)
        try:
            items = []
            for partition_key in partition_keys:
                response = table.query(
                    KeyConditionExpression='user_id_job_id = :user_id_job_id AND #date_c = :date_c',
                    ExpressionAttributeNames={'#date_c': 'date_c'},
                    ExpressionAttributeValues={':user_id_job_id': partition_key[0], ':date_c': partition_key[1]}
                )

                items += response.get('Items', [])
                print("revisar que traigan cout: ", items)
    
                # Si hay un elemento en la lista y el conteo es mayor a 1, restar 1 y actualizar la tabla
                if items and items[-1].get('count_applications', 0) > 0:
                    table.update_item(
                        Key={
                            'user_id_job_id': partition_key[0],
                            'date_c': partition_key[1]
                        },
                        UpdateExpression='SET #count_applications = #count_applications - :decrement',
                        ExpressionAttributeNames={'#count_applications': 'count_applications'},
                        ExpressionAttributeValues={':decrement': 1}
                    )
                    items[-1]['count_applications'] -= 1
        finally:
            print("termino bien! ", locks)
      #      lock_client.release_locks(locks, partition_keys)

    # Retornar el valor de items fuera del bloque
    return {
        'statusCode': 200,
        'body': json.dumps(items, default=default_encoder)
    }
    
def default_encoder(o):
    if isinstance(o, decimal.Decimal):
        return int(o)
    raise TypeError(repr(o) + " is not JSON serializable")