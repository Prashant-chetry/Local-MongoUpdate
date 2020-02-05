const MongoClient = require('mongodb').MongoClient;
const url = process.argv[3] || '';
const fs = require('fs');
const JSONStream = require('JSONStream');
const localUrl = 'mongodb://127.0.0.1:27017/';
const path = process.cwd() + '/data.json'
function main(url, localUrl) {
	const opertaion = process.argv[2] || 'read';
	const name = process.argv[4] || '';
	const collectionName = process.argv[5] || '';
	const limit = parseInt(process.argv[6], 10) || 100;
	const options = process.argv[7] || 'local';
	const write = fs.createWriteStream(path);
	if (opertaion === 'read') {
		MongoClient.connect(localUrl, {useUnifiedTopology: true}, (lerr, lClient)=> {
			if (lerr) console.log(lerr);
			const lDb = lClient.db(name);
			const lCollection =  lDb.collection(collectionName);
			MongoClient.connect(url, {useUnifiedTopology: true}, (err, client)=> {
				if (err) console.error(err);
				try {
					const db = client.db(name);
					const collection = db.collection(collectionName);
					const createdAt = new Date(`1-${new Date().getMonth() - 1}-${new Date().getFullYear()}`);
					if (options === 'json') {
						//using stream
						const docStream = collection.find({createdAt: {$gte: createdAt}}, {limit}).stream();
						docStream.pipe(JSONStream.stringify()).pipe(write);
						docStream.on('end', ()=> {
							client.close();
							let arrayInput = [];
							const read = fs.createReadStream('./data.json');
							read.pipe(JSONStream.parse('*')).on('data', async(data)=> {
								const doc = await lCollection.findOne({_id: data._id});
								if (!doc) {
									arrayInput.push(data);
									if (arrayInput.length === 10) {
										read.pause();
										try {
											const ids = await lCollection.insertMany([...arrayInput]);
											console.log(ids.insertedIds);
											arrayInput = [];
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
							});
						});
					}
					else {
						collection.find({createdAt: {$gte: createdAt}}, {limit}).forEach(async d=> {
							const doc = await lCollection.findOne({_id: d._id});
							if (!doc) lCollection.insert(d);
						});
					}
				}
				catch (err) {
					console.log(err);
					lClient.close();
					client.close();
				}
			});
		});
	}
}
main(url, localUrl);
