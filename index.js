const path = require("path");
const fs = require("fs");
const xls2json = require("xls-to-json");
const sqlite3 = require("sqlite3");
const { open } = require("sqlite");

function createDbConnection(filename) {
  return open({
    filename,
    driver: sqlite3.Database,
  });
}

async function main() {
  try {
    sqlite3.verbose();
    const db = await createDbConnection("./masoutisdb.sqlite");
    await db.exec("DROP TABLE IF EXISTS planograms;");
    await createTable(db);
    readXLS(db);
  } catch (error) {
    console.error(error);
  }
}

main();

// -----------------------------------------------------
async function createTable(db) {
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
  const directoryPath = path.join(__dirname, "planograms");
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
              await insertRecords(db, result, fk);
            }
          }
        );
      }
    });
  });
}

// insert records to table planograms
async function insertRecords(db, json, fk) {
  try {
    let values = "";
    json.forEach((rec) => {
      //get by index as names are in Greek
      var ar = Object.values(rec);
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
