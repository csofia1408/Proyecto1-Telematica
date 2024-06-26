﻿# Proyecto1-Telematica
# Sistema de Almacenamiento Distribuido

Este proyecto implementa un sistema de almacenamiento distribuido que permite la fragmentación, compresión, almacenamiento y recuperación de archivos a través de múltiples nodos de datos.

## Requisitos

- Python 3
- Pika (cliente RabbitMQ)
- Flask (servidor RESTful)
- Requests (para realizar solicitudes HTTP)
- zlib (para la compresión de datos)

## Configuración

### Cliente

El cliente interactúa con el sistema para cargar y descargar archivos. Antes de ejecutar el cliente, asegúrese de haber instalado las dependencias necesarias y de tener acceso a un servidor de RabbitMQ en `localhost`.

Ejecute el cliente utilizando el siguiente comando:

```bash
python cliente.py
```

El cliente solicitará la URL del servidor para establecer la conexión.

### Servidor

El servidor actúa como un nodo de almacenamiento y gestiona las solicitudes del cliente. Asegúrese de que RabbitMQ esté en funcionamiento en el servidor y de haber instalado todas las dependencias necesarias.

Ejecute el servidor utilizando el siguiente comando:

```bash
python servidor.py [puerto]
```

El servidor se ejecutará en el puerto especificado y estará listo para recibir conexiones de clientes.

### Nodos de Datos

Los nodos de datos almacenan fragmentos de archivos y responden a las solicitudes de descarga de archivos. Para ejecutar un nodo de datos, debe especificar su dirección IP y puerto como argumentos de línea de comandos al iniciar el script `data_node.py`.

Ejemplo:

```bash
python data_node.py 8000
```

Con esta configuración, el sistema de almacenamiento distribuido estará listo para funcionar y gestionar las solicitudes de carga y descarga de archivos de manera distribuida.
