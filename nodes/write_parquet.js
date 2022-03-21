var parquet = require('parquetjs-lite');

module.exports = function(RED) {

    function CreateNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;

		const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
		delay(1000).then(() => Parquet(node, config)); /// waiting 1 second.
	}

	function Parquet(node, config) {

		let filename = config.filename;
		let columns = config.columns;

        node.on('input', function(msg) {
			if (msg.filename != undefined) filename = msg.filename;

			writeFile(msg);
			node.send(msg);
        });

		node.on('close', function() {
		});

		function generateSchemaColumns() {
			let schemaColumns = {};
			columns.forEach(element => {
				schemaColumns[element["column"]] = { type: element["type"], optional: true };
			});

			return schemaColumns;
		}

		async function writeFile(msg) {
			let writer;
			try {
				// declare a schema for the table
				let schema = await new parquet.ParquetSchema(generateSchemaColumns());
	
				// create new ParquetWriter that writes to parquet file
				writer = await parquet.ParquetWriter.openFile(schema, filename);
		
				// append a few rows to the file
				await msg["payload"].forEach(element => {
					writer.appendRow(element);
				});
	
				writer.close();
			} catch (error) {
				node.error(error);
				writer.close();
			}
		}
	}

    RED.nodes.registerType("Write Parquet", CreateNode);
}