const path = require("path");
const fs = require("fs");
const xls2json = require("xls-to-json");
const csvToJson = require("convert-csv-to-json");
const sqlite3 = require("sqlite3");
const { open } = require("sqlite");

const http = require("http");
const host = 'localhost';
const port = 8000;

const requestListener = function (req, res) {
    res.writeHead(200);
    res.end("Masoutis service is running");
};

const server = http.createServer(requestListener);
server.listen(port, host, () => {
    console.log(`Server is running on http://${host}:${port}`);

    updateDB();
});

function createDbConnection(filename) {
    return open({
        filename,
        driver: sqlite3.Database,
    });
}

async function updateDB() {
    try {
        sqlite3.verbose();
        const db = await createDbConnection("./masoutisdb.sqlite");

        await db.exec("DROP TABLE IF EXISTS products;");
        await createProductsTable(db);
        readCSV(db);

        await db.exec("DROP TABLE IF EXISTS planograms;");
        await createPlanogramsTable(db);
        readXLS(db);

    } catch (error) {
        console.error(error);
    }
}

// -----------------------------------------------------
async function createPlanogramsTable(db) {
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
}



// Read xls files from planograms
function readXLS(db) {
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
async function insertPlanogramRecords(db, json, fk) {
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
}


async function createProductsTable(db) {
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
}


// Read CSV file
function readCSV(db) {
    const directoryPath = path.join(__dirname, "data");
    fs.readdir(directoryPath, function (err, files) {
        if (err) {
            return console.log("Unable to scan directory: " + err);
        }

        files.forEach(async function ( file) {
            if (path.extname(file) === ".csv") {

                // Append header line to csv and save to a new file in parent folder
                let data = fs.readFileSync(directoryPath + '/barcodes/'+file); // Read existing contents into data
                let fd = fs.openSync(directoryPath + '/'+file, 'w+');
                let buffer = new Buffer.from('PLU|description|price|discount|action|extra\n');

                fs.writeSync(fd, buffer, 0, buffer.length, 0); // Write new data
                fs.writeSync(fd, data, 0, data.length, buffer.length); // Append old data
                fs.close(fd);

                let json = csvToJson.fieldDelimiter('|').getJsonFromCsv(directoryPath + '/'+file);
                await insertBarcodeRecords(db, json);
            }
        });
    });
}

async function insertBarcodeRecords(db, json) {
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
}