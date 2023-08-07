# CoderHouseDE_ProyectoFinal_MatiasCirillo
 Proyecto final para el curso de Data Engineer de CoderHouse

--------------------------------------------------------


## Objetivo
El objetivo del proyecto es cargar datos de clima en una base de Redshift, obteniendo los datos desde una API la cual devuelve un JSON con distinta informacion sobre el clima en ese momento en los paises de Sud America, los datos de los paises lo obtenemos de otra API.

Se realizan dos transformaciones pequeñas en los datos que devuelve la API:
- Conversion del dato last_update a DateTime.
- Conversion del dato wind_direction (int) por un punto cartesiano (string) (N, NE, S, SE, E, SW, W, NW)

Los datoas en la base no pueden estar duplicados, para ello se realiza una verificacion entre el nombre del pais y la ultima fecha de actualizacion de datos que tiene la API, este ultimo cambia cada 15 minutos, por lo que como minimo deberia poder almacenar informacion del clima de los paises de Sud America con un delta de 15 minutos.

## API's
- Paises: https://restcountries.com/v3.1/subregion/
- Clima: http://api.weatherapi.com/v1/current.json

--------------------------------------------------------

## Ejecución

1. Posicionarse en la carpeta raiz. A esta altura debería ver el archivo `docker-compose.yml`.

2. Crear un archivo con variables de entorno llamado `.env` ubicado a la misma altura que el `docker-compose.yml`. Cuyo contenido sea:
```bash
REDSHIFT_HOST=...
REDSHIFT_PORT=5439
REDSHIFT_DB=...
REDSHIFT_USER=...
REDSHIFT_SCHEMA=...
REDSHIFT_PASSWORD=...
REDSHIFT_URL="jdbc:postgresql://${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}?user=${REDSHIFT_USER}&password=${REDSHIFT_PASSWORD}"

DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar

API_KEY=...
```
3. Construyo la imagen de Airflow con el Dockerfile dentro del path `docker_images/airflow/Dockerfile`.

4. Ejecutar el siguiente comando para levantar los servicios de Airflow.
```bash
docker-compose up --build
```
5. Una vez que los servicios estén levantados, ingresar a Airflow en `http://localhost:8080/`.
6. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Redshift:
    * Conn Id: `redshift_default`
    * Conn Type: `Amazon Redshift`
    * Host: `host de redshift`
    * Database: `base de datos de redshift`
    * Schema: `esquema de redshift`
    * User: `usuario de redshift`
    * Password: `contraseña de redshift`
    * Port: `5439`
7. En la pestaña `Admin -> Variables` crear una nueva variable con los siguientes datos:
    * Key: `driver_class_path`
    * Value: `/tmp/drivers/postgresql-42.5.2.jar`
    ---
    * Key: `spark_scripts_dir`
    * Value: `/opt/airflow/scripts`
    ---
    * Key: `SMTP_EMAIL_FROM`
    * Value: `Email del cual queres enviar`
    ---
    * Key: `SMTP_EMAIL_TO`
    * Value: `Email al cual queres recibir`
    ---
    * Key: `SMTP_PASSWORD`
    * Value: `*******`  
    
    (Si usas gmail podes obtener asi tu contraseña de aplicacion: https://support.google.com/mail/answer/185833?hl=en)

8. Encender el DAG `etl_weather` (el DAG se ejecuta automaticamente cada 15 minutos --> hh:05/hh:20/hh:35/hh:50).

