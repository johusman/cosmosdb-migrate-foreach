#!/usr/bin/env node

'use strict';

const commander = require('commander');
const fs = require('fs');
const CosmosClient = require("@azure/cosmos").CosmosClient;
const vm = require('vm');

commander
    .version("0.1.2")
    .usage("<options> [code]")
    .option('-a, --account <account>', "CosmosDB account endpoint url (required)")
    .option('-d, --database <database>', "Database (required)")
    .option('-c, --container <container>', "Container (required)")
    .option('-k, --key <key>', 'CosmosDB database master auth key (required)')
    .option('-f, --file <file>', 'Javascript code to execute for each item (required if code argument missing)')
    .parse(process.argv);

if (!commander.account || !commander.database || !commander.container || !commander.key || (!commander.file && commander.args.length === 0)) {
    commander.help();
}

async function entryPoint(account, databaseId, containerId, key, filePath, code) {
    try {
        if (!code) {
            code = fs.readFileSync(filePath);
        }

        const cosmosClient = new CosmosClient({
            endpoint: account,
            auth: {
            masterKey: key
            }
        });

        const script = new vm.Script(code);

        const database = cosmosClient.database(databaseId);
        const container = database.container(containerId);
        await handleContainer(container, (document, ops) => {
            const sandbox = { document: document, ops: ops };
            script.runInNewContext(sandbox);
        });
    } catch(e) {
        console.log("Something went wrong: ", e);
    }
}

async function handleContainer(container, processor) {
    const iterator = container.items.readAll().getAsyncIterator();
    for await (let response of iterator) {
        const document = response.result;
        const id = document.id;

        /* Probably can't pass async callback code to sandboxed processor, so we queue and do it later */
        let operation = async () => { console.log("Ignoring document " + id); };

        processor(document, {
            delete: (partitionKey) => {
                operation = async () => {
                    console.log("Deleting document " + id);
                    await container.item(id, partitionKey).delete();        
                };
            },
            update: (newVersion) => {
                operation = async () => {
                    console.log("Replacing document " + id);
                    await container.items.upsert(newVersion);
                };
            },
            log: (obj) => {
                operation = async () => {
                    console.log(obj);
                };
            }
        });

        if (operation) {
            await operation();
        }
    }
}

entryPoint(commander.account, commander.database, commander.container, commander.key, commander.file, commander.args[0]);
