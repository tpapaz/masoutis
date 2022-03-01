const path = require("path");
const fs = require("fs");
const xls2json = require("xls-to-json");
const csvToJson = require("convert-csv-to-json");
const readline = require('readline');
const sqlite3 = require("sqlite3");
const ftp = require("basic-ftp");
const AdmZip = require("adm-zip");
const { open } = require("sqlite");
const { ftpConfig } = require("./config.js");
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
                return (obj.type === 1 && obj.name !== 'EshopItems.txt')
            })

            let latest_file = barcodeFiles[barcodeFiles.length-1];

            console.log("LATEST: ", latest_file.name);

            // Replace local file with the one from FTP
            await client.downloadTo(path.join(__dirname, "data/barcodes/mas_new.csv"), "/"+latest_file.name);

            // Get TXT file
            await client.downloadTo(path.join(__dirname, "data/desc/desc.txt"), "/EshopItems.txt");

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
        const db = await createDbConnection(path.join(__dirname, "out/masoutisdb.sqlite"));

        await db.exec("DROP TABLE IF EXISTS products;");
        await createProductsTable(db);
        const descriptions = await parseDescriptions();

        await readCSV(db, descriptions);

        await db.exec("DROP TABLE IF EXISTS planograms;");
        await createPlanogramsTable(db);
        await readXLS(db, descriptions);
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

let parseDescriptions = async () => {

    let descMap = new Map();
    descMap = await processLineByLine(descMap);

    async function processLineByLine(map) {
        const fileStream = fs.createReadStream(path.join(__dirname,'/data/desc/desc.txt'), 'utf16le');

        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity
        });
        // Note: we use the crlfDelay option to recognize all instances of CR LF
        // ('\r\n') in input.txt as a single line break.

        for await (const line of rl) {
            let lineArr = line.split('\t', 2);
            map.set(lineArr[0], lineArr[1]);
        }
        return map;
    }
    return descMap;
}

// Read xls files from planograms
let readXLS = async (db, desc) => {
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

                            Object.keys(result).forEach(function(key){

                                if (desc.get(result[key]['ΦΟΡ. ΚΩΔΙΚΟΣ']) ) {
                                    result[key]['Προϊόν'] = desc.get(result[key]['ΦΟΡ. ΚΩΔΙΚΟΣ'])
                                }
                                result[key]['Προϊόν'] = filterEntryDescription(result[key]['Προϊόν']);

                            });
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
                                                 "initial_price" TEXT,
                                                 "discount" TEXT,
                                                 "discount_label" TEXT,
                                                 "final_price" TEXT,
                                                 "action" TEXT
                       );`);
        console.log("TABLE 'products' created...");
    } catch (error) {
        throw error;
    }
};


// Read CSV file
let readCSV = async (db, desc) => {
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

                Object.keys(json).forEach(function(key) {

                    // Filter label according to the description txt file
                    if (desc.get(json[key]['PLU'])) {
                        json[key]['description'] = desc.get(json[key]['PLU'])
                    }
                    json[key]['description'] = filterEntryDescription(json[key]['description']);

                    json[key]['action'] = json[key]['action'].toLowerCase();
                    json[key]['action'] = json[key]['action'].replace(/(omoia)/g, ' όμοια ');

                    // Add discount label if discount is present
                    json[key]['discount_label'] = parseInt(json[key]['discount'], 10) !== 0 ? String(json[key]['discount']) + '% έκπτωση' : '';

                });
                await insertBarcodeRecords(db, json);
            }
        });
    });
};

let insertBarcodeRecords = async (db, json) => {
    try {
        let values = '';
        json.forEach((rec) => {
            values += `("${rec.PLU}","${rec.description}","${rec.price}","${rec.discount}","${rec.discount_label}","${rec.extra}","${rec.action}"),`;
        });

        values = values.replace(/.$/, ";");
        await db.exec(`INSERT INTO products VALUES ${values}`);
        console.log("Products CSV file inserted");
    } catch (error) {
        console.error(error);
    }
};

let filterEntryDescription = entry => {

    // Γραμμάρια
    if(entry.match(/([0-9]+Γ)|([0-9]+γ)/)) {
        entry = entry.replace(/(ΓΡ. )|(ΓΡ.)/g, ' Γραμμάρια ');
        entry = entry.replace(/(γρ. )|(γρ.)/g, ' Γραμμάρια ');
        entry = entry.replace(/(ΓΡ)$/g, ' Γραμμάρια ');
        entry = entry.replace(/(Γ)$/g, ' Γραμμάρια ');
    }

    // mL
    entry = entry.replace(/(ML)/g, 'mL'); // english M
    entry = entry.replace(/(ΜL)/g, 'mL'); // greek M

    // lt
    entry = entry.replace(/(lt.)/g, ' Λίτρα ');
    entry = entry.replace(/(1L)/g, ' 1 Λίτρο ');

    // Τεμάχια
    if(entry.match(/([0-9]+τεμ)/)) {
        entry = entry.replace(/(τεμ.)/g, ' Τεμάχια ');
    }

    // Mr Grand
    entry = entry.replace(/(MrGrand.)/g, 'Mr. Grand ');

    // Γλουτένη
    entry = entry.replace(/(ΧΓΛΟΥΤΕΝΗ)/g, 'xωρίς γλουτένη');
    entry = entry.replace(/(ΧΓΛΟΥΤ)/g, 'xωρίς γλουτένη');
    entry = entry.replace(/(ΧΓΛ)/g, 'xωρίς γλουτένη');

    entry = entry.replace(/(Χ ΓΛΟΥΤΕΝΗ)/g, 'xωρίς γλουτένη');
    entry = entry.replace(/(Χ ΓΛΟΥΤ)/g, 'xωρίς γλουτένη');
    entry = entry.replace(/(Χ ΓΛΟΥ)/g, 'xωρίς γλουτένη');

    entry = entry.replace(/(ΧΩΡ ΓΛΟΥΤΕΝΗ)/g, 'xωρίς γλουτένη');
    entry = entry.replace(/(ΧΩΡ ΓΛΟΥΤ)/g, 'xωρίς γλουτένη');

    // Χωρίς Ζάχαρη
    entry = entry.replace(/(Χ ΖΑΧ)/g, 'xωρίς ζάχαρη');
    entry = entry.replace(/(ΧΩΡ ΖΑΧΑΡΗ)/g, 'xωρίς ζάχαρη');

    // Δωρο
    entry = entry.replace(/( ΔΩΡ )/g, ' δώρο ');

    // ΦΑΚ
    entry = entry.replace(/( ΦΑΚ )/g, ' φακελάκι ');

    // After all the substitutions make every label lowercase
    entry = entry.toLowerCase();

    // €
    entry = entry.replace(/(€)/g, ' ευρώ ');
    // Replace double quotes with single
    entry = entry.replace(/"/g, "'");

    return entry;
};