# node-red-contrib-parquet
Converts between a PARQUET string and its JavaScript object representation, in either direction.

### Installation
It can be installed from the Node-RED palette, or by executing the following command in the Node-RED installation directory:
```sh
npm install @msigrupo-develop/node-red-contrib-parquet
```

### Dependencies
This package depends on the following libraries:
- [parquetjs-lite][1]

### Usage
Read or write mode must be selected.

##### Read mode
It will parse a single Buffer object from a parquet file to its JavaScript object representation. It is intended to be used together with the file reading node.
- A single message per row
- A single message [array]

By default, the output is sent to msg.payload bu it can be configured.
An [example][2] is ready to use.

##### Write mode
It will parse a JavaScript object to a Parqute single Buffer object. It is intended to be used together with the file writting node.
The columns names and types to be processed from the input must be configured. The input must be an array of objects with the names and values of the columns.
The output returns a single Buffer object in msg.payload.
An [example][3] is ready to use.

### Changelog
Changes can be followed [here][4]

### Credits
This project receives funding in the European Commission’s Horizon 2020 Research Programme under Grant Agreement Number 870062

### License
[MIT][5]


[1]:https://www.npmjs.com/package/parquetjs-lite
[2]:https://github.com/msigrupo/node-red-contrib-parquet/blob/master/examples/ReadParquet.json
[3]:https://github.com/msigrupo/node-red-contrib-parquet/blob/master/examples/WriteParquet.json
[4]:https://github.com/msigrupo/node-red-contrib-parquet/blob/master/CHANGELOG.md
[5]:https://github.com/msigrupo/node-red-contrib-parquet/blob/master/LICENCE