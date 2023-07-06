# CoderHouseDE_ProyectoFinal_MatiasCirillo
 Proyecto final para el curso de Data Engineer de CoderHouse

--------------------------------------------------------
Contenido en .env:

    REDSHIFT_USER='USERNAME'
    REDSHIFT_PASSWORD='PASS'
    REDSHIFT_HOST='HOST.redshift.amazonaws.com'
    REDSHIFT_PORT=5439
    REDSHIFT_DATABASE='DBNAME'
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

```bash
python main.py
```