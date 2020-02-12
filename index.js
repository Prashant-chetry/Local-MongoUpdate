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
	const limit = parseInt(process.argv[6], 10) || 100;
	const options = process.argv[5] || 'local';
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
						const filter = {source: 'HDFC', createdAt: {$gte: new Date('12 feb 2020')}};
						const docStream = collection.find({...filter}, {limit}).stream();
						docStream.pipe(JSONStream.stringify()).pipe(write);
						docStream.on('end', ()=> {
							client.close();
							let arrayInput = [];
							const read = fs.createReadStream('./data.json');
							read.pipe(JSONStream.parse('*')).on('data', async(data)=> {
								const doc = await lCollection.findOne({_id: data._id});
								if (!doc) {
									arrayInput.push(data);
									if (arrayInput.length === 20) {
										read.pause();
										try {
											const ids = await lCollection.insertMany([...arrayInput]);
											console.log(ids.insertedIds);
											arrayInput = [];
											await sleepFun(0.5);
											read.resume();
										}
										catch (err) {
											console.error('error in data insert', err);
											lClient.close()
											;
										}
									}
								}
							}).on('end', ()=> {
								console.log('complete');
								lClient.close();
								fs.unlinkSync(path);
							}).on('error', error=> {
								console.log(error)
							})
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
//node index.js url db-name collection-name 'json' limit