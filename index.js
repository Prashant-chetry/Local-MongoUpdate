const MongoClient = require('mongodb').MongoClient;
const url = process.argv[2] || '';
const fs = require('fs');
const JSONStream = require('JSONStream');
const localUrl = 'mongodb://127.0.0.1:27017/';
const path = process.cwd() + '/data.json'
const sleepFun = function(timeout){
	return new Promise((resolve, reject)=>{
		setTimeout(() => {
			console.log('set timout ' + timeout)
			resolve();
		}, timeout);
	})
}
function main(url, localUrl) {
	const name = process.argv[3] || '';
	const collectionName = process.argv[4] || '';
	const limit = parseInt(process.argv[6], 10) || 1000;
	const options = process.argv[5] || 'local';
	const saveJson = process.argv[7] === 'true' ? true : false;
	const write = fs.createWriteStream(path);
		MongoClient.connect(localUrl, {useUnifiedTopology: true}, (lerr, lClient)=> {
			if (lerr) console.log(lerr);
			const lDb = lClient.db(name);
			const lCollection = lDb.collection(collectionName);
			MongoClient.connect(url, {useUnifiedTopology: true}, (err, client)=> {
				if (err) console.error(err);
				try {
					const db = client.db(name);
					const collection = db.collection(collectionName);
					const createdAt = new Date(`1-${new Date().getMonth() - 1}-${new Date().getFullYear()}`);
					if (options === 'json') {
						//using stream
						// const filter = {};
						const docStream = collection.find({}, {limit}).stream();
						docStream.pipe(JSONStream.stringify()).pipe(write);
						docStream.on('end', ()=> {
							client.close();
							lClient.close();
							const exec = require('child_process').exec;
							exec(`mongoimport --db ${name} --collection ${collectionName} --file ${path} --jsonArray`, (error, stdout, stderr)=> {
								if(error) console.error(error);
								if(stdout) console.log(stdout);
								if(stderr) console.log(stderr);
								if(!saveJson) fs.unlinkSync(path);
							});
						});
					}
					else {
						collection.find({createdAt: {$gte: createdAt}}, {limit: 1000}).forEach(async d=> {
							const doc = await lCollection.findOne({_id: d._id});
							if (!doc){
								lCollection.insert(d);
								await sleepFun(0.5);
							}
						});
					}
				}
				catch (err) {
					console.log(err);
					// lClient.close();
					// client.close();
				}
			});
		});
	}
main(url, localUrl);

//arguments in terminal
// in url cmd replace it with live url of mongodb
//in db-name replace it with database name of live mongo
// in collection-name replace it with collection name of live mongo 
///if 'json' the script will create a file and use that file for data insertion using stream else directly insert in mongodb using loop
// limit replace it with 1000 or any number of your desire
// saveJson replace it will 'true' for kiping the json file else replace it will 'false'
//node index.js url db-name collection-name 'json' limit saveJSon