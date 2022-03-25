var parquet = require('parquetjs-lite');
var fs = require('fs');

module.exports = function(RED) {

	function CreateNode(config) {
		RED.nodes.createNode(this,config);
		var node = this;

		const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
		delay(1000).then(() => Parquet(node, config)); /// waiting 1 second.
	}

	function Parquet(node, config) {

		let reader, writer;

		let option = config.option; //read || write

		//write
		let columns = config.columns;

		//read
		let columnsToRead = config.rcolumns.split(',');
		let multi = config.multi; //multiple || single
		let outputPty = config.outputPty; //Ouput property to send

		//Check if columns has '' element and delete
		columnsToRead = columnsToRead.filter(function(e) { return e !== '' });

		node.on('input', function(msg) {
			node.inputMsg = JSON.parse(JSON.stringify(msg));
			if (option == "read") {
				//Read
				readFile(msg);
			} else if (option == "write") {
				//Write
				writeFile();
			}
		});

		node.on('close', function() {
			if (option == "read")  {
				//Close reader if is not close
				closeReader();
			} else if (option == "write") {
				closeWriter();
			}
		});
		
		//Read parquet file from buffer
		async function readFile(msg) {
			try {
				// create new ParquetReader that reads from buffer
				await openBuffer(msg.payload);
	
				// create a new cursor
				let cursor = reader.getCursor(columnsToRead);

				//Clear "payload" to not pass the buffer
				delete msg.payload;
	
				// read all records from the file and print them
				let record = null;
				let msgRecords = [];
				while (record = await cursor.next()) {
					if (multi == 'single') msgRecords.push(record);
					else if (multi == 'multiple') readNodeSend(msg, record);
				}
	
				if (multi == 'single') readNodeSend(msg, msgRecords);
	
				closeReader();				
			} catch (error) {
				closeReader();
				node.error(error);
			}
		}

		// create new ParquetReader that reads from buffer
		async function openBuffer(buffer) {
			reader = await parquet.ParquetReader.openBuffer(buffer);
		}

		function readNodeSend(msg, records) {
			//Prepare output property
			var splitOutput = outputPty.split('.');
			var splitFirstLevel = splitOutput.shift();
			let strres = recursiveOuputSplit(records, splitOutput, "");

			let msgSend = JSON.parse(JSON.stringify(msg));
			msgSend[splitFirstLevel] = JSON.parse(strres);
			
			node.send(msgSend);
		}

		function recursiveOuputSplit(records, outputTxtSplit, txtRecursive) {
			if (outputTxtSplit.length == 0) {
				//Check if ouputPty has more than one level
				let hasLevel = false;
				if (outputPty.split('.').length > 1) hasLevel = true;
				
				txtRecursive = txtRecursive + JSON.stringify(records);
				if (hasLevel) txtRecursive = '{' + txtRecursive;
				
				return txtRecursive;
			}   
			else {  
				var deleted = outputTxtSplit.shift();
				if (outputTxtSplit.length == 0) txtRecursive = txtRecursive +'"'+deleted+'"' + ':';
				else txtRecursive = txtRecursive +'"'+deleted+'"' + ': {';
				
				txtRecursive = recursiveOuputSplit(records, outputTxtSplit, txtRecursive);
				txtRecursive = txtRecursive + '}';
				
				return txtRecursive;
			}
		}

		async function closeReader() {
			//Close reader if is not close
			try {
				await reader.close();
			} catch (error) {
				
			}
		}

		async function writeFile() {
			try {
				// declare a schema for the table
				let schema = await new parquet.ParquetSchema(generateSchemaColumns());
	
				let tmpFilename = "tmp_" + new Date().getTime() + ".parquet";

				// create new ParquetWriter that writes to parquet file
				writer = await parquet.ParquetWriter.openFile(schema, tmpFilename);
		
				// append a few rows to the file
				await node.inputMsg["payload"].forEach(element => {
					writer.appendRow(element);
				});

				await closeWriter();

				//Read file to get buffer
				setTimeout(() => {
					getFileBuffer(tmpFilename);
				}, 2000);
			} catch (error) {
				node.error(error);
				await closeWriter();
			}
		}

		function generateSchemaColumns() {
			let schemaColumns = {};
			columns.forEach(element => {
				schemaColumns[element["column"]] = { type: element["type"], optional: true };
			});

			return schemaColumns;
		}

		//Read file to get buffer
		async function getFileBuffer(tmpFilename) {
			try {
				var data = fs.readFileSync(tmpFilename);

				node.inputMsg["payload"] = data;
				node.send(node.inputMsg);

				await deleteTmpFile(tmpFilename);
			} catch (error) {
				node.error(error);
			}
		}

		function deleteTmpFile(tmpFilename) {
			try {
				fs.unlinkSync(tmpFilename);
			} catch (error) {
				node.error(error);
			}
		}

		async function closeWriter() {
			//Close reader if is not close
			try {
				writer.close();
			} catch (error) {
				
			}
		}
	}

	RED.nodes.registerType("parquet", CreateNode);
}