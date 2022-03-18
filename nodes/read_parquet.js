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
		let output = config.output; //single: a single message [array] || multiple: a message per row
		let columns = config.columns.split(','); //Columns to read
		let outputPty = config.outputPty; //Ouput property to send

		//Check if columns has '' element and delete
		var indexEmpty = columns.indexOf('');
		if (indexEmpty !== -1) {
			columns.splice(indexEmpty, 1);
		}

		let reader;

		node.status({fill:"green",shape:"dot",text:""});

        node.on('input', function(msg) {
			if (msg.filename != undefined) filename = msg.filename;
			if (msg.output != undefined) output = msg.output;
			if (msg.columns != undefined) columns = msg.columns.split(',');

			openFile();
			readFile(msg);
        });

		node.on('close', function() {
			//Close reader if is not close
			closeFile();
		});

		async function openFile() {
			reader = await parquet.ParquetReader.openFile(filename);
		}

		async function readFile(msg) {
			// create new ParquetReader that reads from parquet file
			await openFile();

			//Get file rows
			let rowCount = reader.getRowCount().toString();

			// create a new cursor
			let cursor = reader.getCursor(columns);

			// read all records from the file and print them
			let record = null;
			let currentRow = 1;
			let msgRecords = [];
			while (record = await cursor.next()) {
				if (output == 'single') msgRecords.push(record);
				else if (output == 'multiple') NodeSend(msg, record);

				node.status({fill:"green",shape:"dot",text:currentRow + "/" + rowCount});
				currentRow += 1;
			}

			if (output == 'single') NodeSend(msg, msgRecords);

			closeFile();
		}

		function NodeSend(msg, records) {
			//Prepare output property
			var splitOutput = outputPty.split('.');
			var splitFirstLevel = splitOutput.shift();
			let strres = RecursiveOuputSplit(records, splitOutput, "");

			msg[splitFirstLevel] = JSON.parse(strres);
			node.send(msg);
		}

		function RecursiveOuputSplit(records, outputTxtSplit, txtRecursive) {
			if (outputTxtSplit.length == 0) {
				txtRecursive = txtRecursive + JSON.stringify(records);
				txtRecursive = '{' + txtRecursive;
				return txtRecursive;
			}   
			else {  
				var eliminado = outputTxtSplit.shift();
				if (outputTxtSplit.length == 0) txtRecursive = txtRecursive +'"'+eliminado+'"' + ':';
				else txtRecursive = txtRecursive +'"'+eliminado+'"' + ': {';
				
				txtRecursive = RecursiveOuputSplit(records, outputTxtSplit, txtRecursive);
				txtRecursive = txtRecursive + '}';
				
				return txtRecursive;
			}
		}

		async function closeFile() {
			//Close reader if is not close
			try {
				await reader.close();
			} catch (error) {
				
			}
		}
	}

    RED.nodes.registerType("Read Parquet", CreateNode);
}