import csv
import boto3
from pymongo import MongoClient

mongo_host = '98.83.127.213'  # Cambiar si es necesario
mongo_port = 27017  # El puerto en el que corre MongoDB
mongo_db = 'compras'
mongo_collection_detallecarritos = 'detallecarritos'  # Colección de detallecarritos
mongo_collection_carritos = 'carritos'  # Colección de carritos

# Parámetros para S3
nombre_bucket = "contenedor3zamir"
fichero_carritos_total = 'detallecarritos_total.csv'
fichero_carritos_detalles = 'detallecarritos_detalles.csv'
fichero_productos = 'productos_carrito.csv'
fichero_carritos = 'carritos.csv'  # Nuevo CSV para carritos

# Conectar a MongoDB
try:
    client = MongoClient(mongo_host, mongo_port)
    db = client[mongo_db]

    # Colección de detallecarritos
    collection_detallecarritos = db[mongo_collection_detallecarritos]
    print(f'Conectado a la base de datos MongoDB {mongo_db}, colección {mongo_collection_detallecarritos}')

    # Extraer datos de la colección detallecarritos
    registros = list(collection_detallecarritos.find())

    # Generar archivo CSV para detallecarritos (total por carrito)
    with open(fichero_carritos_total, 'w', newline='') as archivo_carritos_total:
        escritor_carritos_total = csv.writer(archivo_carritos_total)
        for registro in registros:
            escritor_carritos_total.writerow([registro['_id'], registro['total']])

    print(f'Registros de totales de detallecarritos guardados en {fichero_carritos_total}')

    # Generar archivo CSV para detallecarritos (detalles por producto en cada carrito)
    with open(fichero_carritos_detalles, 'w', newline='') as archivo_carritos_detalles:
        escritor_carritos_detalles = csv.writer(archivo_carritos_detalles)
        for registro in registros:
            for producto in registro['productos']:
                escritor_carritos_detalles.writerow([registro['_id'], producto['producto_id'], producto['cantidad'], producto['precioUnitario']])

    print(f'Registros de detalles de detallecarritos guardados en {fichero_carritos_detalles}')

    # Generar archivo CSV para productos del carrito
    with open(fichero_productos, 'w', newline='') as archivo_productos:
        escritor_productos = csv.writer(archivo_productos)
        for registro in registros:
            for producto in registro['productos']:
                escritor_productos.writerow([producto['producto_id'], producto['cantidad'], producto['precioUnitario'], registro['_id']])

    print(f'Registros de productos guardados en {fichero_productos}')

    # *** NUEVA SECCIÓN: Generar archivo CSV para los carritos ***
    collection_carritos = db[mongo_collection_carritos]
    print(f'Conectado a la base de datos MongoDB {mongo_db}, colección {mongo_collection_carritos}')

    carritos = list(collection_carritos.find())

    # Generar archivo CSV para carritos (con los atributos _id, usuario_id, carritoDetalle_id, estado, fecha)
    with open(fichero_carritos, 'w', newline='') as archivo_carritos:
        escritor_carritos = csv.writer(archivo_carritos)
        escritor_carritos.writerow(['_id', 'usuario_id', 'carritoDetalle_id', 'estado', 'fecha'])  # Encabezado

        for carrito in carritos:
            escritor_carritos.writerow([
                carrito['_id'],
                carrito['usuario_id'],
                carrito.get('carritoDetalle_id', ''),  # Obtener carritoDetalle_id, si no existe dejarlo vacío
                carrito['estado'],
                carrito['fecha']
            ])

    print(f'Registros de carritos guardados en {fichero_carritos}')

except Exception as e:
    print(f'Error al conectar a MongoDB o al generar los CSV: {e}')

finally:
    if 'client' in locals() and client:
        client.close()
        print('Conexión a MongoDB cerrada')

# Subir archivos a S3 en directorios separados
s3 = boto3.client('s3')

# Subir CSV de totales de detallecarritos
if fichero_carritos_total:
    try:
        ruta_s3_carritos_total = f'detallecarritos_total/{fichero_carritos_total}'
        s3.upload_file(fichero_carritos_total, nombre_bucket, ruta_s3_carritos_total)
        print(f'Archivo {fichero_carritos_total} subido al bucket en la ruta {ruta_s3_carritos_total}')
    except Exception as e:
        print(f'Error al subir {fichero_carritos_total} a S3: {e}')

# Subir CSV de detalles de detallecarritos
if fichero_carritos_detalles:
    try:
        ruta_s3_carritos_detalles = f'detallecarritos_detalles/{fichero_carritos_detalles}'
        s3.upload_file(fichero_carritos_detalles, nombre_bucket, ruta_s3_carritos_detalles)
        print(f'Archivo {fichero_carritos_detalles} subido al bucket en la ruta {ruta_s3_carritos_detalles}')
    except Exception as e:
        print(f'Error al subir {fichero_carritos_detalles} a S3: {e}')

# Subir CSV de productos del carrito
if fichero_productos:
    try:
        ruta_s3_productos = f'productos_carrito/{fichero_productos}'
        s3.upload_file(fichero_productos, nombre_bucket, ruta_s3_productos)
        print(f'Archivo {fichero_productos} subido al bucket en la ruta {ruta_s3_productos}')
    except Exception as e:
        print(f'Error al subir {fichero_productos} a S3: {e}')

# Subir CSV de carritos a S3
if fichero_carritos:
    try:
        ruta_s3_carritos = f'carritos/{fichero_carritos}'
        s3.upload_file(fichero_carritos, nombre_bucket, ruta_s3_carritos)
        print(f'Archivo {fichero_carritos} subido al bucket en la ruta {ruta_s3_carritos}')
    except Exception as e:
        print(f'Error al subir {fichero_carritos} a S3: {e}')
