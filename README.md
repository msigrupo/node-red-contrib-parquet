# node-red-contrib-parquet
Nodo de [Node-RED][1] que lee y genera ficheros con formato parquet.


### Instalación
Puede instalar desde la paleta de Node-RED buscando `node-red-contrib-parquet`, o bien ejecutando el comando en el directorio de instalación de Node-RED.
```sh
npm install node-red-contrib-parquet
```


### Dependencias
Este paquete depende de las siguientes librerías:
- [parquetjs-lite][2]


### Uso del nodo Write Parquet
Utilice para la creación de ficheros de tipo parquet, indicando las columnas a crear.
Puede ver [un ejemplo][3] preparado para usarse.

##### Salidas
Devuelve el mismo mensaje que se le ha pasado en la entrada.


### Uso del nodo Read Parquet
Utilice para la lectura de ficheros de tipo parquet.
Puede ver [un ejemplo][4] preparado para usarse.

##### Salidas
En el nodo Read Parquet se puede configurar para que devuelva un mensaje por línea, o bien un mensaje por fichero. La salida se debe configurar en el propio nodo.


## Licencia
[MIT][5]

[1]:http://nodered.org
[2]:https://www.npmjs.com/package/parquetjs-lite
[3]:https://github.com/msigrupo/node-red-contrib-parquet/examples/WriteParquet.json
[4]:https://github.com/msigrupo/node-red-contrib-parquet/examples/ReadParquet.json
[5]:https://github.com/msigrupo/node-red-contrib-parquet/LICENCE