const path = require("path");
const fs = require("fs");
const xls2json = require("xls-to-json");
const csvToJson = require("convert-csv-to-json");
const sqlite3 = require("sqlite3");
const ftp = require("basic-ftp")
const { open } = require("sqlite");
const { ftpConfig} = require("./config.js");
const cron = require("node-cron");
const express = require("express");
const app = express();

app.use(express.static('html'));

app.get('/', function(req, res) {
    res.sendFile(__dirname + '/index.html');
});

app.post('/update', async function(request, response){

    console.log("Started update");
    await updateFiles();
    await updateDB();
    response.send("Update successful!");
    console.log("Update successful");
});

app.listen(4343);


cron.schedule('0 0 *!/2 * * *', async () => {
    console.log('Running a job every 2 hrs');

    await updateFiles();
    await updateDB();

}, {
    scheduled: true,
    timezone: "Europe/Athens"
});

let updateFiles = async () => {

    async function connect() {
        const client = new ftp.Client()
        client.ftp.verbose = true
        try {

            await client.access(ftpConfig)
            let list = await client.list();

            // Delete planogram files from local directory
            let planogramDir = path.join(path.join(__dirname, "data/planograms"));
            fs.readdir(planogramDir, (err, files) => {
                if (err) throw err;

                for (const file of files) {

                    if (path.extname(file) === ".xls") {
                        fs.unlink(path.join(planogramDir, file), err => {
                            if (err) throw err;
                        });
                    }
                }
            });

            // Copy planograms from ftp to local dir
            await client.downloadToDir(path.join(__dirname, "data/planograms"), 'evision ΠΛΑΝΟ');

            // Get all files (not directories)
            let barcodeFiles = list.filter(obj => {
                return (obj.type === 1)
            })

            // Get latest file based on date modified.
            let latest_file = barcodeFiles.reduce(function (r, a) {
                return r.rawModifiedAt > a.rawModifiedAt ? r : a;
            });

            // Replace local file with the one from FTP
            await client.downloadTo(path.join(__dirname, "data/barcodes/mas_new.csv"), "/"+latest_file.name);

        }
        catch(err) {
            console.log(err);
        }
        client.close();
    }

    await connect();
}

let createDbConnection = filename => {
    return open({
        filename,
        driver: sqlite3.Database,
    });
};

let updateDB = async () => {
    try {
        sqlite3.verbose();
        const db = await createDbConnection("./masoutisdb.sqlite");

        await db.exec("DROP TABLE IF EXISTS products;");
        await createProductsTable(db);
        await readCSV(db);

        await db.exec("DROP TABLE IF EXISTS planograms;");
        await createPlanogramsTable(db);
        await readXLS(db);

    } catch (error) {
        console.error(error);
    }
}

// -----------------------------------------------------
let createPlanogramsTable = async db => {
    try {
        await db.exec(`CREATE TABLE planograms (
                                                   "fk" TEXT NOT NULL,
                                                   "shelf_num" TEXT ,
                                                   "position" TEXT,
                                                   "description" TEXT,
                                                   "face" TEXT,
                                                   "view" TEXT,
                                                   "PLU" TEXT NOT NULL
                       );`);
        console.log("TABLE 'planograms' created...");
    } catch (error) {
        throw error;
    }
};

// Read xls files from planograms
let readXLS = async db => {
    const directoryPath = path.join(__dirname, "data/planograms");
    fs.readdir(directoryPath, function (err, files) {
        if (err) {
            return console.log("Unable to scan directory: " + err);
        }

        files.forEach(function (file) {
            if (path.extname(file) === ".xls") {
                const fk0 = file.split("_")[0];
                const fk = fk0.split(" ")[0];
                xls2json(
                    {
                        input: directoryPath + "/" + file, // input xls
                        rowsToSkip: 6,
                        allowEmptyKey: false,
                    },
                    async function (err, result) {
                        if (err) {
                            console.log(err);
                        } else {
                            await insertPlanogramRecords(db, result, fk);
                        }
                    }
                );
            }
        });
    });
}

// insert records to table planograms
let insertPlanogramRecords = async (db, json, fk) => {
    try {
        let values = "";
        json.forEach((rec) => {
            //get by index as names are in Greek
            let ar = Object.values(rec);
            if (ar[5])
                values += `("${fk}","${ar[0]}","${ar[1]}","${ar[2]}","${ar[3]}","${ar[4]}","${ar[5]}"),`;
        });

        values = values.replace(/.$/, ";");
        await db.exec(`INSERT INTO planograms VALUES ${values}`);
        console.log(fk, " inserted");
    } catch (error) {
        console.error(error);
    }
};


let createProductsTable = async db => {
    try {
        await db.exec(`CREATE TABLE products (
                                                 "PLU" TEXT,
                                                 "description" TEXT,
                                                 "price" TEXT,
                                                 "discount" TEXT,
                                                 "action" TEXT,
                                                 "extra" TEXT
                       );`);
        console.log("TABLE 'products' created...");
    } catch (error) {
        throw error;
    }
};


// Read CSV file
let readCSV = async db => {
    const directoryPath = path.join(__dirname, "data/barcodes");
    const parentPath = path.join(__dirname, "data");
    fs.readdir(directoryPath, function (err, files) {
        if (err) {
            return console.log("Unable to scan directory: " + err);
        }

        files.forEach(async function ( file) {

            if (path.extname(file) === ".csv") {

                // Append header line to csv and save to a new file in parent folder
                let data = fs.readFileSync(directoryPath + '/'+file); // Read existing contents into data
                let fd = fs.openSync(parentPath + '/'+file, 'w+');
                let buffer = new Buffer.from('PLU|description|price|discount|action|extra\n');

                fs.writeSync(fd, buffer, 0, buffer.length, 0); // Write new data
                fs.writeSync(fd, data, 0, data.length, buffer.length); // Append old data
                fs.closeSync(fd);

                let json = csvToJson.fieldDelimiter('|').getJsonFromCsv(parentPath + '/'+file);
                await insertBarcodeRecords(db, json);
            }
        });
    });
};

let insertBarcodeRecords = async (db, json) => {
    try {
        let values = '';
        json.forEach((rec) => {
            values += `('${rec.PLU}','${rec.description}','${rec.price}','${rec.discount}','${rec.action}','${rec.extra}'),`;
        });

        values = values.replace(/.$/, ";");
        await db.exec(`INSERT INTO products VALUES ${values}`);
        console.log("Products CSV file inserted");
    } catch (error) {
        console.error(error);
    }
};