import csv
import boto3
from pymongo import MongoClient

mongo_host = '98.83.127.213'  # Cambiar si es necesario
mongo_port = 27017  # El puerto en el que corre MongoDB
mongo_db = 'compras'
mongo_collection = 'detallecarritos'

# Parámetros para S3
nombre_bucket = "contenedor3zamir"
fichero_carritos = 'detallecarritos.csv'
fichero_productos = 'productos_carrito.csv'

# Conectar a MongoDB
try:
    client = MongoClient(mongo_host, mongo_port)
    db = client[mongo_db]
    collection = db[mongo_collection]
    print(f'Conectado a la base de datos MongoDB {mongo_db}, colección {mongo_collection}')

    # Extraer datos de la colección
    registros = list(collection.find())

    # Generar archivo CSV para detallecarritos
    with open(fichero_carritos, 'w', newline='') as archivo_carritos:
        escritor_carritos = csv.writer(archivo_carritos)
        escritor_carritos.writerow(['detallecarrito_id', 'total'])  # Encabezado

        for registro in registros:
            escritor_carritos.writerow([registro['_id'], registro['total']])

    print(f'Registros de detallecarritos guardados en {fichero_carritos}')

    # Generar archivo CSV para productos
    with open(fichero_productos, 'w', newline='') as archivo_productos:
        escritor_productos = csv.writer(archivo_productos)
        escritor_productos.writerow(['producto_id', 'cantidad', 'precioUnitario', 'detallecarrito_id'])  # Encabezado

        for registro in registros:
            for producto in registro['productos']:
                escritor_productos.writerow([producto['producto_id'], producto['cantidad'], producto['precioUnitario'], registro['_id']])

    print(f'Registros de productos guardados en {fichero_productos}')

except Exception as e:
    print(f'Error al conectar a MongoDB o al generar los CSV: {e}')

# Subir archivos a S3 si se han generado correctamente
s3 = boto3.client('s3')

if fichero_carritos:
    try:
        s3.upload_file(fichero_carritos, nombre_bucket, fichero_carritos)
        print(f'Archivo {fichero_carritos} subido al bucket {nombre_bucket}')
    except Exception as e:
        print(f'Error al subir {fichero_carritos} a S3: {e}')

if fichero_productos:
    try:
        s3.upload_file(fichero_productos, nombre_bucket, fichero_productos)
        print(f'Archivo {fichero_productos} subido al bucket {nombre_bucket}')
    except Exception as e:
        print(f'Error al subir {fichero_productos} a S3: {e}')
