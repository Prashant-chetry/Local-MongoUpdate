const MongoClient = require('mongodb').MongoClient;
const util = require('util')
const url = process.argv[3] || '';
const fs = require('fs');
const JSONStream = require('JSONStream')
const localUrl = 'mongodb://127.0.0.1:27017/';
function main(url, localUrl) {
	const opertaion = process.argv[2] || 'read';
	const name = process.argv[4] || '';
	const collectionName = process.argv[5] || '';
	const limit= parseInt(process.argv[6], 10) || 100;
	const options = process.argv[7] || 'local';
	const write = fs.createWriteStream(process.cwd() + '/data.json');
	if(opertaion === 'read'){
			MongoClient.connect(localUrl, {useUnifiedTopology: true}, (lerr, lClient)=> {
		if(lerr) console.log(lerr)
		const lDb = lClient.db(name);
		const lCollection =  lDb.collection(collectionName);
	MongoClient.connect(url, { useUnifiedTopology: true },(err, client)=> {
		if(err) console.error(err);
		try{
		const db = client.db(name);
		const collection = db.collection(collectionName);
		const createdAt = new Date(`1-${new Date().getMonth() -1}-${new Date().getFullYear()}`);
		if(options === 'json'){
			collection.find({createdAt: {$gte: createdAt}}, {limit}).stream().pipe(JSONStream.stringify()).pipe(write);
		}else{
			collection.find({createdAt: {$gte: createdAt}}, {limit}).forEach(async d=> {
			const doc = await lCollection.findOne({_id: d._id});
			console.log(doc, 'da')
			if(!doc) lCollection.insert(d);
		});
	}
		}catch(err){
			console.log(err)
			lClient.close();
			client.close();
		}
	})
	});
	}else{
		MongoClient.connect(localUrl, { useUnifiedTopology: true }, (err, client)=> {
			if(err) console.log(err);
			try{
				const db = client.db(name);
				const collection = db.collection(collectionName);
				fs.readFile(process.cwd() + '/data.txt', (err, data)=> {
					console.log(data.toString())
					// collection.insertMany(data.toString());
				})
			}catch(error){console.log(error)}
		});
	}
}
main(url, localUrl);