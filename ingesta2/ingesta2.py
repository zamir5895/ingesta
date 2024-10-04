import psycopg2
import csv
import boto3

fichero_csv = None

# Parámetros de conexión a PostgreSQL
host = '98.83.127.213'
port = 8006  
database = 'gestion'
user = 'postgres'
password = 'utec'
tabla = 'producto'

try:
    # Conectar a la base de datos PostgreSQL
    conexion = psycopg2.connect(
        host=host,
        port=port,  
        database=database,
        user=user,
        password=password
    )

    print(f'Conectado a la base de datos {database}')
    cursor = conexion.cursor()

    # Ejecutar consulta para obtener los datos
    cursor.execute(f'SELECT * FROM {tabla}')
    registros = cursor.fetchall()
    columnas = [desc[0] for desc in cursor.description]

    # Generar archivo CSV
    fichero_csv = 'producto.csv'
    with open(fichero_csv, 'w', newline='') as archivo_csv:
        escritor_csv = csv.writer(archivo_csv)
        escritor_csv.writerow(columnas)
        escritor_csv.writerows(registros)

    print(f'Registros guardados en {fichero_csv}')

except psycopg2.Error as error:
    print(f'Error al conectar a PostgreSQL: {error}')
finally:
    # Cerrar la conexión a la base de datos si está abierta
    if 'conexion' in locals() and conexion:
        cursor.close()
        conexion.close()
        print('Conexión PostgreSQL cerrada')

# Subir el archivo CSV a S3
if fichero_csv:
    nombreBucket = "contenedor2zamir"
    s3 = boto3.client('s3')

    try:
        response = s3.upload_file(fichero_csv, nombreBucket, fichero_csv)
        print(f'Archivo {fichero_csv} subido al bucket {nombreBucket}')
    except Exception as e:
        print(f'Error al subir el archivo a S3: {e}')
else:
    print("El archivo CSV no fue generado, no se subirá a S3.")
