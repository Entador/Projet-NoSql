import redis
import mysql.connector
import time

def setup_redis():
    # Créer une connexion à Redis
    redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
    return redis_client

def setup_mysql():
    # Créer une connexion à MySQL
    mysql_conn = mysql.connector.connect(
        host='localhost',
        user='your_username',
        password='your_password',
        database='your_database'
    )
    return mysql_conn

def insert_data_redis(redis_client, key, value):
    start_time = time.time()
    redis_client.set(key, value)
    end_time = time.time()
    return end_time - start_time

def retrieve_data_redis(redis_client, key):
    start_time = time.time()
    value = redis_client.get(key)
    end_time = time.time()
    return value, end_time - start_time

def update_data_redis(redis_client, key, new_value):
    start_time = time.time()
    redis_client.set(key, new_value)
    end_time = time.time()
    return end_time - start_time

def insert_data_mysql(mysql_conn, cursor, table, data):
    insert_query = f"INSERT INTO {table} (key, value) VALUES (%s, %s)"
    start_time = time.time()
    cursor.execute(insert_query, data)
    mysql_conn.commit()
    end_time = time.time()
    return end_time - start_time

def retrieve_data_mysql(cursor, table, key):
    select_query = f"SELECT value FROM {table} WHERE key = %s"
    start_time = time.time()
    cursor.execute(select_query, (key,))
    result = cursor.fetchone()
    end_time = time.time()
    return result, end_time - start_time

def update_data_mysql(mysql_conn, cursor, table, key, new_value):
    update_query = f"UPDATE {table} SET value = %s WHERE key = %s"
    start_time = time.time()
    cursor.execute(update_query, (new_value, key))
    mysql_conn.commit()
    end_time = time.time()
    return end_time - start_time

def main():
    # Configuration pour Redis
    redis_client = setup_redis()
    redis_key = 'mykey'
    redis_value = 'Hello, Redis!'

    # Configuration pour MySQL
    mysql_conn = setup_mysql()
    mysql_cursor = mysql_conn.cursor()
    mysql_table = 'mytable'
    mysql_data = (redis_key, redis_value)

    # Performances d'insertion
    redis_insert_time = insert_data_redis(redis_client, redis_key, redis_value)
    mysql_insert_time = insert_data_mysql(mysql_conn, mysql_cursor, mysql_table, mysql_data)

    print(f"Insertion time (Redis): {redis_insert_time} seconds")
    print(f"Insertion time (MySQL): {mysql_insert_time} seconds")

    # Performances de recherche
    redis_retrieve_result, redis_retrieve_time = retrieve_data_redis(redis_client, redis_key)
    mysql_retrieve_result, mysql_retrieve_time = retrieve_data_mysql(mysql_cursor, mysql_table, redis_key)

    print(f"Retrieval time (Redis): {redis_retrieve_time} seconds, Result: {redis_retrieve_result}")
    print(f"Retrieval time (MySQL): {mysql_retrieve_time} seconds, Result: {mysql_retrieve_result}")

    # Performances de mise à jour
    new_value = 'Updated value'
    redis_update_time = update_data_redis(redis_client, redis_key, new_value)
    mysql_update_time = update_data_mysql(mysql_conn, mysql_cursor, mysql_table, redis_key, new_value)

    print(f"Update time (Redis): {redis_update_time} seconds")
    print(f"Update time (MySQL): {mysql_update_time} seconds")

    # Fermer les connexions
    redis_client.close()
    mysql_cursor.close()
    mysql_conn.close()

if __name__ == "__main__":
    main()
