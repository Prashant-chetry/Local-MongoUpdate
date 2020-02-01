const MongoClient = require('mongodb').MongoClient;
const util = require('util')
const url = process.argv[2] || '';
const fs = require('fs');
// const Readable = require('stream').Readable;
// const JSONStream = require('JSONStream')
const localUrl = 'mongodb://127.0.0.1:27017/';
function main(url, localUrl) {
	const name = process.argv[3] || '';
	const collectionName = process.argv[4] || '';
	const limit= parseInt(process.argv[5], 10) || 100;
	const write = fs.createWriteStream(process.cwd() + '/data.txt');
	MongoClient.connect(localUrl, {useUnifiedTopology: true}, (lerr, lClient)=> {
		if(lerr) console.log(lerr)
		const lDb = lClient.db(name);
		const lCollection =  lDb.collection(collectionName);
	MongoClient.connect(url, { useUnifiedTopology: true },(err, client)=> {
		if(err) console.error(err);
		try{
		const db = client.db(name);
		const collection = db.collection(collectionName);
		collection.find({}, {limit}).forEach(d=> {
			console.log(d);
			if(!lCollection.find({_id: d._id})) lCollection.insert(d);
		})
		}catch(err){
			console.log(err)
			lClient.close();
			client.close();
		}
	})
	});
}
main(url, localUrl);