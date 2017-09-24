/**
 * @Main: readAndImportDynamoDB.js
 */
'use strict';
var AWS = require("aws-sdk");
var LineByLineReader = require('line-by-line');
var replaceall = require("replaceall");
var log4js = require('log4js');
var fs = require('fs');
var atob = require('atob');
var sleep = require('sleep');

//datetime config
var dateTime = require('node-datetime');
var dt = dateTime.create();
var formattedDateTime = dt.format('Y-m-d_H_M_S');
var LOOP_DELAY_SEC = 3;
var LOOP_DELAY_LINE_COUNT = 100;
var SOURCE_FILE_NAME = "";
var LOG4JS_FILE_PATH = "logFile_"+formattedDateTime+".log";
var ERROR_DATA_FILE_PATH = "errorData_"+formattedDateTime+".txt";
var targetTableName = "";
var dynamoDB = new AWS.DynamoDB({region : "xxxxxx" , maxRetries : 3, retryDelayOptions : {base : 1200}});
var currentData = "";
var totalErrorCount = 0;
var totalInsertCount = 0;
var count = 0;
var fileIdExistSkipCount = 0;

//log4js config
var logger = log4js.getLogger('readAndImportDynamoDB');
logger.level = 'info';
log4js.configure({
    appenders: {
        import_dynamodb: {
          type: 'file', filename: LOG4JS_FILE_PATH
        }
    },
  categories: {
        default: { 
            appenders: ['import_dynamodb'], level: 'info'
        }
    }
});

//file append config
var logErrorData = fs.createWriteStream(ERROR_DATA_FILE_PATH, {
    encoding: 'utf8',
    flags: 'a' // 'a' means appending (old data will be preserved)
});

/**
 * @function start
 * @purpose code starting point
 */
async function start() {
    process.on('exit', await exitHandler.bind());
    logger.info("================== START ==================");
    try {
        /**
         * 1.first arg : targetTableName & source filename
         */
        if (process.argv.length > 2) {
            targetTableName = process.argv[2];
            SOURCE_FILE_NAME = targetTableName;
            logger.info("args: " + targetTableName);
            if(!fs.existsSync(targetTableName)) {
                console.log("ERROR: targetTableName not exsits....");
                process.exit(1);
            }
        }else{
            console.log("ERROR: no args....");
            console.log("please type the target table name.");
            process.exit(1);
        }

        //Synchronous processing of lines
        let lr = new LineByLineReader(SOURCE_FILE_NAME);

        /**
         * LineByLineReader read event
         */
        await lr.on('line',async function (line) {
            
            try {
                count++;

                //check if need to sleep
                let checkSleep = count % LOOP_DELAY_LINE_COUNT;
                if(checkSleep == 0 && count != 0){
                    logCurrentCounts();
                    sleep.sleep(LOOP_DELAY_SEC);
                }

                //replace dynamodb key to uppercase
                line = replaceall('{"b":','{"B":',line);
                line = replaceall('{"bs":','{"BS":',line);
                line = replaceall('{"bool":','{"BOOL":',line);
                line = replaceall('{"s":','{"S":',line);
                line = replaceall('{"ss":','{"SS":',line);
                line = replaceall('{"n":','{"N":',line);
                line = replaceall('{"ns":','{"NS":',line);
                line = replaceall('{"null":','{"NULL":',line);
                line = replaceall('{"l":','{"L":',line);
                line = replaceall('{"m":','{"M":',line);
                currentData = line;
                
                let insertObj = JSON.parse(line);
                let dynamodbKey = insertObj.fileId.B;

                //check if data exist by fileId
                var getParams = {
                    "TableName" : targetTableName,
                    "Key": {
                        "fileId" : {
                            "B" : insertObj.fileId.B
                        }
                    },
                    "AttributesToGet" : [
                        "fileId"
                    ]                        
                }

                //get data by fileId
                dynamoDB.getItem(getParams,function (err,data) {
                    if(err){
                        totalErrorCount++;
                        logger.error("getItem , " + origonFileId + " , " + currentData + " , complete error on next line...");
                        logger.error(err);
                        logCurrentCounts();
                        logErrorData.write(currentData + "\r\n");
                    }else{
                        if(!("Item" in data)){
                            //no data exist in target table , start insert...
                            let insertParams = {
                                "TableName": targetTableName,
                                "Item": insertObj
                            }

                            dynamoDB.putItem(insertParams,function (err) {
                                if(err) {
                                    totalErrorCount++;
                                    logger.error("putItem , " + origonFileId + " , " + currentData + " , complete error on next line...");
                                    logger.error(err);
                                    logCurrentCounts();
                                    logErrorData.write(currentData + "\r\n");
                                }else{
                                    totalInsertCount++;
                                }
                            });
                        }else{
                            fileIdExistSkipCount++;
                        }
                    }
                });
            }catch(err){
                totalErrorCount++;
                logger.error(origonFileId + " , " + currentData + " , complete error on next line...");
                logger.error(err);
                logCurrentCounts();
                logErrorData.write(currentData + "\r\n");
            }
        });

        /**
         * LineByLineReader end event
         */
        await lr.on('end', async function () {
            console.log("source file ended and closed...");
        });

        /**
         * LineByLineReader error event
         */
        await lr.on('error', async function (err) {
            logger.error(err);
        });

    }catch(err) {
        logger.error("Exception out of file line loop");
        logCurrentCounts();
        logger.error(err); 
    }
}

/**
 * @function logCurrentCounts
 * @purpose log the current counts
 */
async function logCurrentCounts() {
    logger.info("total error count: " + totalErrorCount);
    logger.info("total insert count: " + totalInsertCount);
    logger.info("total fileId exsit skip count: " + fileIdExistSkipCount);
    logger.info("executed count: " + count);
}

/**
 * @function exitHandler
 * @purpose when exit event
 */
async function exitHandler() {
    if(totalErrorCount > 0){
        console.log("finsihed and ended with error...");
    }else{
        console.log("finsihed and all success!");
    }
    logErrorData.end();
    logger.info("================== FINISHED ==================\r\ntotal error count: " + totalErrorCount + "\r\ntotal insert count: " + totalInsertCount + "\r\ntotal fileId skip count: " + fileIdExistSkipCount + "\r\nexecuted count: " + count);
}

//starting point
start();