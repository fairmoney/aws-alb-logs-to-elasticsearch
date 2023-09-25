/*
 * This project based on https://github.com/awslabs/amazon-elasticsearch-lambda-samples
 * Sample code for AWS Lambda to get AWS ELB log files from S3, parse
 * and add them to an Amazon Elasticsearch Service domain.
 *
 *
 * Copyright 2015- Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at http://aws.amazon.com/asl/
 * or in the "license" file accompanying this file.  This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

/* Imports */
const AWS = require('aws-sdk');
const LineStream = require('byline').LineStream;
const BatchStream = require('batch-stream');
const parse = require('alb-log-parser');  // alb-log-parser  https://github.com/igtm/node-alb-log-parser
const path = require('path');
const stream = require('stream');
const zlib = require('zlib');

let indexTimestamp = new Date().toISOString().replace(/\-/g, '.').replace(/T.+/, '');
const esEndpoint = process.env['es_endpoint'];
var endpoint = '';
if (!esEndpoint) {
    console.error('ERROR: Environment variable es_endpoint not set');
} else {
    endpoint =  new AWS.Endpoint(esEndpoint);
}

const region = process.env['region'];
if (!region) {
    console.error("Error. Environment variable region is not defined")
}

const is_local = process.env['is_local'];

const indexPrefix = process.env['index'] || 'elblogs';
const index = indexPrefix + '-' + indexTimestamp; // adds a timestamp to index. Example: elblogs-2016.03.31
const bulkPath = '_bulk';

/* Globals */
const s3 = new AWS.S3();
let totLogLines = {};    // Total number of log lines in the file
let numDocsAdded = {};   // Number of log lines added to ES so far

/*
 * The AWS credentials are picked up from the environment.
 * They belong to the IAM role assigned to the Lambda function.
 * Since the ES requests are signed using these credentials,
 * make sure to apply a policy that permits ES domain operations
 * to the role.
 */
const creds = new AWS.EnvironmentCredentials('AWS');

console.log('Initializing AWS Lambda Function');

/*
 * Get the log file from the given S3 bucket and key.  Parse it and add
 * each log record to the ES domain.
 */
function s3LogsToES(bucket, key, context) {
    // Note: The Lambda function should be configured to filter for .log files
    // (as part of the Event Source "suffix" setting).
    if (!esEndpoint) {
        const error = new Error('ERROR: Environment variable es_endpoint not set')
        context.fail(error);
    }

    // Initialize specific counters
    totLogLines[key] = 0;
    numDocsAdded[key] = 0;

    const s3Stream = s3.getObject({Bucket: bucket, Key: key}).createReadStream();
    const gunzip = zlib.createGunzip();
    const lineStream = new LineStream();
    const batch = new BatchStream({size: 100});

    // Extract the object.key prefix
    const key_prefix = key.split('/')[0]; // Assuming the prefix is the first segment

    const recordStream = new stream.Transform({objectMode: true});
    recordStream.logLines = 0;

    recordStream._transform = function(buffer, encoding, done) {
        let batch = [];
        for (const line of buffer) {
            let logLine = line.toString();
            let logRecord;
            if (logLine.match(/^\{/)) {
                logRecord = JSON.parse(logLine)
                logRecord.old_timestamp = logRecord.timestamp

                if (Number.isInteger(logRecord.timestamp)) {
                    logRecord.timestamp = new Date(logRecord.timestamp)
                } else {
                    console.log(`Fixing incorrect timestamp ${logRecord.timestamp}`);
                    logRecord.timestamp = new Date();
                }
            } else {
                logRecord = parse(logLine);
                // Prevent "illegal_argument_exception" parsing errors
                if (logRecord.matched_rule_priority === '-') {
                    logRecord.matched_rule_priority = 0;
                }
                //Check if the prefix is other than AWSLogs, which means no prefix
                if (key_prefix !== 'AWSLogs') {
                  // Add the 'cluster_name' key-value pair
                  logRecord.cluster_name = key_prefix;
                }
            }
            let serializedCommand = JSON.stringify({"index": {"_index": index}})
            batch.push(serializedCommand)
            let serializedLogRecord = JSON.stringify(logRecord)
            batch.push(serializedLogRecord)
            this.logLines ++;
        }
        let msg = batch.join("\n")+"\n";
        this.push(msg);
        done();
    }
    recordStream._flush = function(done) {
        totLogLines[key] = this.logLines;
        console.log(`Document ${key} has ${totLogLines[key]} records`)
        done();
    }

    // Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
    s3Stream
        .pipe(gunzip)
        .pipe(lineStream)
        .pipe(batch)
        .pipe(recordStream)
        .on('data', function(parsedEntry) {
            postDocumentToES(parsedEntry, context, key);
        })
        .on('error', function() {
            const error = new Error(
                'Error getting object "' + key + '" from bucket "' + bucket + '".  ' +
                'Make sure they exist and your bucket is in the same region as this function.');
            context.fail(error);
        });
}

/*
 * Add the given document to the ES domain.
 * If all records are successfully added, indicate success to lambda
 * (using the "context" parameter).
 */
function postDocumentToES(doc, context, key) {
    var req = new AWS.HttpRequest(endpoint);

    req.method = 'POST';
    req.path = path.join('/', index, bulkPath);
    req.region = region;
    req.body = doc;
    req.headers['presigned-expires'] = false;
    req.headers['Host'] = endpoint.host;
    req.headers['Content-Type'] = 'application/json';

    if (is_local) {
        // local dev credentials admin:admin
        req.headers['Authorization'] = 'Basic YWRtaW46YWRtaW4=';
    } else {
        // Sign the request (Sigv4)
        let signer = new AWS.Signers.V4(req, 'es');
        signer.addAuthorization(creds, new Date());
    }

    // Post document to ES
    var send = new AWS.NodeHttpClient();
    send.handleRequest(req, null, function(httpResp) {
        var body = '';
        httpResp.on('data', function (chunk) {
            body += chunk;
        });
        httpResp.on('end', function (chunk) {
            // console.log('Response body:', body);  // Print the response body from Elasticsearch/OpenSearch
            let _body = JSON.parse(body);

            numDocsAdded[key] += _body.items.length;
            console.log(`Published ${numDocsAdded[key]} of ${totLogLines[key]} documents`)
            if (numDocsAdded[key] === totLogLines[key]) {
                // Mark lambda success.  If not done so, it will be retried.
                console.log(`All ${numDocsAdded[key]} log records added to index ${index} in region ${region}.`);
                context.succeed();
            }
        });
    }, function(err) {
        console.log(`Error: ${err}`);
        console.log(`Response: ${err.response}`);  // Print the response body from Elasticsearch/OpenSearch
        console.log(`${numDocsAdded[key]}  of ${totLogLines[key]}  log records added to ES.`);
        context.fail();
    });
}

/* Lambda "main": Execution starts here */
exports.handler = function(event, context) {
    console.log(`Received event: ${JSON.stringify(event, null, 2)}`);

    event.Records.forEach(function(record) {
        let bucket = record.s3.bucket.name;
        let objKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
        s3LogsToES(bucket, objKey, context);
    });
}
