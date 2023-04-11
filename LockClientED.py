from datetime import datetime
import uuid
import time   

class LockClientED:
    def __init__(self, dynamo_client, table_name):
        self.dynamo_client = dynamo_client
        self.table_name = table_name
        
    def __enter__(self):
        return self
    
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            print("An exception occurred in the with statement.")
            for partition_key, lock_key in zip(self.partition_keys, self.locks):
                try:
                    response = self.dynamo_client.update_item(
                        TableName=self.table_name,
                        Key={
                            "user_id_job_id": {"S": str(partition_key[0])},
                            "date_c": {"S": str(partition_key[1])},
                        },
                    #    UpdateExpression="REMOVE lock_key",
                        UpdateExpression="REMOVE lock_key, expiration_time",
                        ReturnValues="UPDATED_NEW"
                    )
                    print(response)
                except Exception as e:
                    print(f"An error occurred while deleting item: {e}")
        else:
            for partition_key, lock_key in zip(self.partition_keys, self.locks):
                # ...código para liberar bloqueos...
                print("user_id_job_id", str(partition_key))
    
                try:
                    response = self.dynamo_client.update_item(
                        TableName=self.table_name,
                        Key={
                            "user_id_job_id": {"S": str(partition_key[0])},
                            "date_c": {"S": str(partition_key[1])},
                        },
                    #    UpdateExpression="REMOVE lock_key, expiration_time",
                        ReturnValues="UPDATED_NEW"
                    )
                    print(response)
                except Exception as e:
                    print(f"An error occurred while deleting lock_key: {e}")
        return False



    def acquire_locks(self, partition_keys, item_values):
        self.locks = []
        self.partition_keys = partition_keys
        self.partition_key = partition_keys[0]
        self.date_str = partition_keys[0][1]
        date_obj = datetime.strptime(self.date_str, '%Y-%m-%d')
        print("fecha: ", date_obj.strftime('%Y-%m-%d'))
        try:
            for partition_key, item_value in zip(partition_keys, item_values):
                print("count: ", item_value['count_applications'])
                print("entro 1: ", partition_key)
                lock_key = str(uuid.uuid4()) # use a random UUID as lock key
                print(lock_key)
                response = self.dynamo_client.transact_write_items(
                    TransactItems=[
                        {
                            'Put': {
                                'TableName': self.table_name,
                                'Item': {
                                    'user_id_job_id': {'S': partition_key[0]},
                                    'date_c': {'S': partition_key[1]},
                                    'count_applications' :{'N':str(item_value['count_applications'])},
                                    'lock_key': {'S': lock_key},
                                    'expiration_time': {'N': str(int(time.time() + 60))}
                                },
                                'ConditionExpression': '(attribute_not_exists(lock_key) OR attribute_not_exists(expiration_time) OR expiration_time < :now) AND count_applications = :count_value',
                                'ExpressionAttributeValues': {
                                    ':count_value': {'N': str(item_value['count_applications'])},
                                    ':now': {'N': str(int(time.time()))}
                                }

                            }
                        }
                    ]
                )
                self.locks.append(lock_key)
        except Exception as e:
            # Si hay un error al adquirir un bloqueo
            print("Error: ", e)
            raise e
        return self
    
