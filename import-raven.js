#!/opt/node/bin/node
"use strict";

/*******************************************************************************
    import-raven.js

    Read an uncompressed RavenDB dump file and import the elements of 
    array "Docs" into a Postgres table named "col" with a jsonb field "doc":

    create table col (doc jsonb);

    Uses a read-stream so the RavenDB dump file can be arbitrarily large
    without risking exceeding memory or Node's built-in limitations.

    chris@1006.org 

*******************************************************************************/


// input file:
let source = "ravendump.json";

// target postgres URL:
let target = "postgresql://chris:nopass@localhost:5432/chris";


// no need to edit stuff below this line //////////////////////////////////////

let js = require("JSONStream");
let fs = require("fs");
let es = require("event-stream");
let pg = require("pg-native");

let client = new pg(target);
client.connectSync();
client.querySync("BEGIN");
client.prepareSync("insert_doc", "INSERT INTO col (doc) VALUES ($1::jsonb)", 1)

let cnt = 0;
let stream = fs.createReadStream(source);

stream
.pipe(js.parse("Docs.*"))
.pipe(
    es.mapSync( (data) => {
        cnt++;
        if (cnt % 100 === 0) {
            process.stdout.write("\r" + cnt + "   ");
        }
        client.executeSync("insert_doc", [JSON.stringify(data)]);
    })
);

stream.on("end", () => { 
    client.querySync("COMMIT");
    client.querySync("ANALYZE col");
    client.end( () => {
        process.stdout.write("\rDONE: " + cnt + " docs imported!\n");
    })
});

