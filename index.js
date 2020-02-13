const MongoClient = require('mongodb').MongoClient;
const url = process.argv[2] || '';
const fs = require('fs');
const JSONStream = require('JSONStream');
const localUrl = 'mongodb://127.0.0.1:27017/';
const path = process.cwd() + '/data.json';
const sleepFun = function(timeout) {
	return new Promise((resolve, reject)=>{
		setTimeout(() => {
			console.log('set timout ' + timeout);
			resolve();
		}, timeout);
	});
};
const readline = require('readline').createInterface({
	input: process.stdin,
	output: process.stdout,
});
const questionPrompt = (question, options)=> new Promise((resolve, reject)=> {
	readline.question(question + '\n', (value)=> {
		switch(typeof options){
			case 'string': {
				if(options === 'string'){
					if(/[0-9]/.test(value)) return reject(`input must be ${options}`);
					return resolve(value.toLowerCase());
				}
				else if(options === 'number'){
					if(/[A-Za-z]/.test(value)) return  reject(`input must be ${options}`);
					 return resolve(value.toLowerCase());
				}
			}
			case 'object': {
				if(Array.isArray(options)){
				if(!(options || []).includes(value.toLowerCase())) return reject(`input must be ${options}`);
				return resolve(value.toLowerCase());
				}
			}
			default: return reject('not valid');
		}
	});
});
async function main(url, localUrl) {
	try{
	const name = await questionPrompt('Database name', 'string');
	const collectionName = await questionPrompt('Collection name', 'string');
	// const filters = await questionPrompt('filters for collection', 's');
	const limit = await questionPrompt('data limit in number', 'number');
	const options = await questionPrompt('options available for mongo operation [json/local]', ['json', 'local']);
	const saveJson = await questionPrompt('save the json file [yes/no]', ['yes', 'no']);
	const csv = await questionPrompt('csv file creation [yes/no]', ['yes', 'no']);
	const write = fs.createWriteStream(path);
	if (options === 'json') {
		MongoClient.connect(url, {useUnifiedTopology: true}, (err, client)=> {
			if (err) console.error(err);
			try {
				const db = client.db(name);
				const collection = db.collection(collectionName);
				const createdAt = new Date(`1-${new Date().getMonth() - 1}-${new Date().getFullYear()}`);
				//using stream
				// const filter = {};
				const docStream = collection.find({}, {limit: parseInt(limit, 10)}).stream();
				docStream.pipe(JSONStream.stringify()).pipe(write);
				docStream.on('end', ()=> {
					client.close();
					const exec = require('child_process').exec;
					exec(`mongoimport --db ${name} --collection ${collectionName} --file ${path} --jsonArray`, (error, stdout, stderr)=> {
						if (error) console.error(error);
						if (stdout) console.log(stdout);
						if (stderr) console.log(stderr);
						if (saveJson === 'no') fs.unlinkSync(path);
					});
					if(csv.toLowerCase() === 'yes') return;
					readline.close();
					return;
				});
			}
			catch (err) {
				console.log(err);
				client.close();
			}
		});
	}
	else{
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
				collection.find({createdAt: {$gte: createdAt}}, {limit: 1000}).forEach(async d=> {
					const doc = await lCollection.findOne({_id: d._id});
					if (!doc) {
						lCollection.insert(d);
						await sleepFun(0.5);
					}
				});
			}
			catch (err) {
				console.log(err);
				// lClient.close();
			}
		});
	});
	}
}catch(err){
	console.log(err);
	readline.close();
	return;}
}
main(url, localUrl);

//arguments in terminal
// in url cmd replace it with live url of mongodb
//in db-name replace it with database name of live mongo
// in collection-name replace it with collection name of live mongo
///if 'json' the script will create a file and use that file for data insertion using stream else directly insert in mongodb using loop
// limit replace it with 1000 or any number of your desire
// saveJson replace it will 'true' for kiping the json file else replace it will 'false'
//node index.js url
