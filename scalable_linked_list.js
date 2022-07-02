/* Copyright (C) Julien Dollon - All Rights Reserved
 * Unauthorized use or copying of this file is strictly prohibited
 * Proprietary and confidential
 * Written by Julien Dollon <julien@dollon.net>, March 2017
 */
/*
A distributed, horizontally scalable, best ordering, dead-lock free, space/cost optimized, Linked List built on top of DynamoDB

#Introduction:
DynamoDB allows to store key value efficiently with strong consistency and consistent partitioning on the primary key.
We have the need to represents the data in form of a linked-list, and we want to benefits from the same guarantee as dynamo (availability, consistency, latency…).
So we built this facade on top. Each linked list is represented by multiple “page”. 

The first page of a LinkedList contains a pointer to the current last “page” of the linkedlist (where new items should be appended), it is called the “summary” page.
The next pages, also called “data page” contains the data appended to the linkedlist. Each page can contain an array of item and can store between 0 to N item.
N needs to be set by calling “configureMaximumNumberOfElementPerPage“.
The goal of putting together those items in the same node is to not have to create a new dynamo element for each writes. Why? because we want to avoid to have to update the pointer in the summary page too often. This avoid doing too much random access write on the summary page, which on top of being slower, will be a “hotspot” prone to bug at scale. N threads using the linkedlist could increment the page summary in the same time which would lead to a lot of “blank” page.
This linkedlist incorporate automatic retries to dynamo and don’t use any blocking mechanism (C-A-S only).
This linkedlist only uses the primary key of the item to make sure the partitioning is done well.

#Warnings
- Because of race condition, the append algorithm is doing only a “best” effort ordering, which mean you can have value that is slightly (few seconds) older in a later page than the current page (just ignore, or reorder time client side).
- A page can be empty, or not well initialized. It is by design due to concurrent calls. If so, just ignore it and keep iterating through pages to retrieve data.
- Your client can be throttled (4XX Error), listen for that and retry late with exponential back off.
*/

// NOTE: All the assertion and exception managed isn't done here, by design, please manage it well in the caller side

'use strict';

const AWS = require('aws-sdk');
var dynamoDb = {};

const dynamoUtils = require('dynamo_data_access');
const dynamoRetriever = dynamoUtils.item_retriever();
const dynamoIncrementer = dynamoUtils.item_incrementer();

/* This is to initialize the DB name and region you will be targeting. */
module.exports.configureDynamoDB = function(region, table) {
    AWS.config.region = region;
    config.region = region;
    config.tableName = table;
    dynamoDb = new AWS.DynamoDB.DocumentClient();
}

/* This function is to optimize how much a data can be added by “page”.
If page == 1 (which is the minimum) it will create a new dynamodb item for each node in the linkedlist. 
This will most likely not react well at scale (empty page, pointer incremented too much…).

The goal is to put as much data as possible in a single page but:
- The size should not go being the maximum dynamo item size (which is 400 KB)
- BulkRetrieve in dynamo is limited to 16 000KB. You will most likely want to retrieve linkedlist in bulk, so keep this in mind.
- Each page contains 1KB of metadata
- Concurrent threads can be trying to add in the same page despite the limit being reached! It should be small window of few seconds when this happen, but my advice would be to over provisioning by assuming the maximum size of the dynamo DB item is 200kb (and then tune things up or down depending on the scale)
- Overall, the number you put here, is the maximum of TPS for this instance of the linkedlist (req in the average same seconds) you can have before any of your customers get TooBigItemException

So for example, if the maximum size of value is 1 000 characters, in UTF-8, it mean each element maximum 1,074 bytes (1kb).
The maximum size of a page should be: 1KB + 1kb * N <= 400KB, N should be less than 390ish.
Then we apply the over-provisioning factor of /2, so we limit ourself to 200 elements.
Note: it would limit to retrieve 80 linked-list page in a BulkRetrieve operation */
module.exports.configureMaximumNumberOfElementPerPage = function(numberOfMaximumElement) {
    if (numberOfMaximumElement) {
        config.maxElementPerPage = numberOfMaximumElement;
    }
}

/* This will create the first summary page. This operation is idempotent. */
module.exports.idempotentCreate = function(id, metadata) {
    console.log('Creating LinkedList: ' + id);
    const summaryObject = defaultPageSummary(id, metadata);
    console.log('Creating LinkedList Page: ' + summaryObject.id);
    const itemInfo = {
        TableName: config.tableName,
        Item: summaryObject,
        ConditionExpression: '#i <> :val', //CAS on the id for idempotency
        ExpressionAttributeNames: {'#i' : 'id'},
        ExpressionAttributeValues: {':val' : summaryObject.id},
        ReturnValues: 'ALL_OLD'
    };
    return dynamoDb.put(itemInfo).promise().then(res => summaryObject);
}

/*
If the page does not exists it first atomically increment the pointer to point to the “current” page.
This pointer might be incremented as the same time as another thread which is why you can have “blank” page that you should ignore.
It then creates the new page by using the CAS mechanism. If the page get has been created in between, it just fails silently.
Then it finally append the value (or directly append if the page was already created) by inserting the data into the page atomically.
This operation is not idempotent and could insert duplicates due to retries.
Note: value needs to be an object, as we append extra propery to it
*/
module.exports.atomicAppend = function(id, value) {
    return getCurrentPage(id).then(function(currentPage) {
        console.log('CurrentPage is ' + currentPage + ' starting inserting');
        return atomicAppendImpl(id, currentPage, value);
    });
}

/* Not implemented yet */
module.exports.atomicBulkAppendBulk = function() {
    throw new Error();
}

/* This retrieve what is the current page (which is also the ‘last’ page) on which you should write (or stop reading). */
module.exports.getCurrentPage = function(id) {
    return getCurrentPage(id);
}

/* Get the page, can be use to check if a page exists */
module.exports.getPage = function(id, pageId) {
    return getPageData(id, pageId);
}

/* This retrieve the entire content of a page and return all the elements contained into it. */
module.exports.retrieve = function(id, pageId) {
    const addPageIdInResult = function(data) {
        return {
            page_id: pageId+''.replace('_',''),
            data: data
        }
    }

    if ('_' + pageId === constants.labels.summary) {
        return getPageData(id, constants.labels.summary).then(addPageIdInResult);
    }
    else {
        return retrieveDataList(id, pageId).then(addPageIdInResult);
    }
}

/* It will retrieve the top N most recent item that has been appended to the linkedlist.
Be careful to bufferoverflow here. Avoid asking for a crazy amount */
module.exports.retrieveLastMostRecent = function(id, numberOfItems, callback) {
    getCurrentPage(id)
    .then(function(currentPage) {
        retrieveNElement(id, currentPage, null, numberOfItems, callback);
    });
}

/*
put null into fromSequence to retrieve from the maximum item in the list
*/
const retrieveNElement = function(id, fromPage, fromSequence, numberOfItems, callback) {
    var retrievedData = [];
    const recursivelyRetrieveData = function(currentPage, completedCallback) {
        if (currentPage < 0) {
            completedCallback();
            return;
        }
        console.log('Requesting page: ' + currentPage + ' # of items found for now:' + retrievedData.length);
        retrieveDataList(id, currentPage)
        .then(function(data_list) {
            if (data_list && data_list.length > 0) {
                console.log('DB returning ' + data_list.length);
                if (fromSequence) {
                    console.log('Cutting data above the sequence id: ' + fromSequence);
                    data_list.splice(fromSequence, data_list.length - fromSequence);
                    console.log('Data after cut: ' + data_list.length);
                }
                retrievedData = retrievedData.concat(data_list.reverse());
            }
            if (retrievedData.length < numberOfItems) {
                fromSequence = undefined;
                recursivelyRetrieveData(currentPage - 1, completedCallback);
                return;
            }
            else {
                completedCallback();
                return;
            }
        });
    }
    recursivelyRetrieveData(fromPage, function() {
        if (retrievedData.length > numberOfItems) {
            retrievedData = retrievedData.slice(0, numberOfItems);
        }
        callback(retrievedData);
    });
}

/* It will retrieve the next N most recent item that has been appended to the linkedlist. 
To use this, you need to specify where the algorithm should start reading.
Each object retrieved from the Linkedlist contains a PageId, and a SequenceId, this is what
you need to use to generate the pointer to dictate where to start reading.
*/
module.exports.retrieveNextMostRecent = function(id, startAfterPointer, numberOfItems, callback) {
    if (!startAfterPointer ||
        !startAfterPointer.page_id ||
        !startAfterPointer.sequence_id) {
        throw 'No valid pointer has been set';
    }

    const pointer = getValidPointer(startAfterPointer);
    retrieveNElement(id, pointer.page_id, pointer.sequence_id, numberOfItems, callback);
}

const getValidPointer = function(startAfterPointer) {
    console.log('Pointer received:' + JSON.stringify(startAfterPointer));

    var currentPageToStartWith = parseInt(startAfterPointer.page_id);
    var currentSequenceToStartWith = parseInt(startAfterPointer.sequence_id);

    if (currentSequenceToStartWith <= 0) {
        currentSequenceToStartWith = null;
        currentPageToStartWith = currentPageToStartWith - 1;
    }

    if (currentPageToStartWith < 0) {
        currentPageToStartWith = 0;
        currentSequenceToStartWith = 0;
    }

    console.log('currentPageToStartWith: ' + currentPageToStartWith + ' currentSequenceToStartWith: ' + currentSequenceToStartWith);

    return {
        page_id: currentPageToStartWith,
        sequence_id: currentSequenceToStartWith
    };
}

module.exports.getConstants = function() {
    return constants;
}

module.exports.getCurrentConfiguration = function() {
    return config;
}

/* This will allow customer to have a pointer inside the linkedlist
This is useful when they want to get the NEXT elements after calling GetLastElements */
const indexDataList = function(data_list, currentPage, resource_id_parent) {
    if (!data_list) {
        return null;
    }

    var i = 0;
    data_list.forEach(function (item) {
        item.page_id = currentPage + '';
        item.sequence_id = i + '';
        item.resource_id_parent = resource_id_parent;
        i++;
    });
    return data_list;
}

const retrieveDataList = function(id, pageId) {
    console.log('Retrieving from list:' + id + ' page: ' + pageId);
    return getPageData(id, '_' + pageId, 'data_list')
    .then(function(data) {
        console.log('Data retrieved');

        return indexDataList(data.data_list, pageId, id);
    })
    .catch(function(err) {
        console.log('No data found, silently failing:' + err);
        return undefined;
    });
}

const atomicAppendImpl = function(id, currentPage, value, attempt = 0) {
    if (attempt > 1) {
        throw new Error();
    }

    return appendDataToPage(id, currentPage, value)
        .then(function(numberOfElementInPage) {
            console.log('Append successful');

            const result = { page_id: currentPage, sequence_id: numberOfElementInPage - 1 };

            if (numberOfElementInPage >= config.maxElementPerPage) {
                console.log('Increasing capacity of the linkedlist');
                return increasePageCounter(id, currentPage).then(function(incrementedCurrentPage) {
                    if (incrementedCurrentPage && incrementedCurrentPage > currentPage) {
                        return createNewPage(id, incrementedCurrentPage).then(function() {
                            return result;
                        });
                    }
                    else {
                        //Race condition, ignoring.
                        return result;
                    }
                });
            }
            else {
                console.log('No need to increase capacity, # of element in page:' + numberOfElementInPage + ' and max is: ' + config.maxElementPerPage);
                return result;
            }

        })
        .catch(err => {
            if (err.code === 'ValidationException') {
                console.log('Page does not exists, creating it: ' + JSON.stringify(err));
                return createNewPage(id, currentPage).then(function() {
                    return atomicAppendImpl(id, currentPage, value, attempt + 1);
                });
            }
            else {
                console.log('Unknown Error happened: ' + JSON.stringify(err));
                throw new Error();
            }
        });
}

const increasePageCounter = function(id, currentCounterValue) {
    const tableName = config.tableName;
    const key = { 'id': id + constants.labels.summary };

    return dynamoIncrementer.incrementCounter(config.region, config.tableName, key, 'currentPage', currentCounterValue)
        .then(function(res) {
        console.log('Summary has been incremented, new currentPage: ' + res.Attributes.currentPage);
        return res.Attributes.currentPage;
    }).catch(function(err) {
        if (err && err.code === 'ConditionalCheckFailedException') {
            console.log('CurrentPage already been incremented due to race condition, ignoring');
            return undefined;
        }
        else {
            console.log('Error happened when trying to increment CurrentPage: ' + JSON.stringify(err));
            throw new Error();
        }
    });
}

const createNewPage = function(id, pageId) {
    console.log('Creating a page for:' + id + ' id: ' + pageId);
    const itemId = id + '_' + pageId;
    const pageData = getDefaultPageData(itemId);
    const pageInfo = {
        TableName: config.tableName,
        Item: pageData,
        ConditionExpression: '#i <> :val', //CAS
        ExpressionAttributeNames: {'#i' : 'id'},
        ExpressionAttributeValues: {':val' : itemId},
        ReturnValues: 'ALL_OLD'
    };
    return dynamoDb.put(pageInfo).promise()
        .then(res => {
            console.log('Page created: ' + pageId);
        })
        .catch(err => {
            if (err.code === 'ConditionalCheckFailedException') {
                console.log('Error page already exists; most likely a race condition, ignoring');
            }
            else {
                console.log('Error happened in creating new page: ' + JSON.stringify(err));
                throw new Error();
            }
        });
}

const getDefaultPageData = function(itemId) {
    const timestamp = new Date().getTime();
    return {
        v: 1,
        id: itemId,
        submittedAt: timestamp,
        data_list: []
    };
}

const getCurrentPage = function(id) {
    return getPageData(id, constants.labels.summary, 'currentPage')
        .then(function(data) {
            console.log('Retrieved currentPage:' + data.currentPage);
            return data.currentPage;
        });
}

const appendDataToPage = function(id, pageId, data) {
    console.log('Appending a message to LinkedList: ' + id + ' page: ' + pageId);

    const insertImpl = function() {
        const itemId = id + '_' + pageId;
        const atomicAppendListUpdate = {
            TableName: config.tableName,
            Key: { 'id': itemId },
            UpdateExpression : 'SET #attrName = list_append(#attrName, :attrValue)',
            ExpressionAttributeNames : {
            '#attrName' : 'data_list'
            },
            ExpressionAttributeValues : {
            ':attrValue' : [data]
            },
            ReturnValues: 'ALL_NEW'
        };
        return dynamoDb.update(atomicAppendListUpdate).promise();
    }

    return insertImpl()
        .then(res => {
            return res.Attributes.data_list.length;
        });
}

const getPageData = function(id, pageId, specificFieldToFilterOn) {
    return dynamoRetriever.getDynamoItem(config.region,
                                         config.tableName,
                                         'id',
                                         id + pageId,
                                         specificFieldToFilterOn)
                        .then(function(data) {
                            var returnValue = undefined;
                            if (data && data.Item) {
                                returnValue = data.Item;
                            }
                            return returnValue;
                        });
}

const config = {
    tableName: '',
    region: '',
    maxElementPerPage: 50 //default
}

const constants = {
    labels: {
        summary: '_summary',
        publicSummary: 'summary'
    }
}

const defaultPageSummary = function(id, metadata) {
    const timestamp = new Date().getTime();
    var summary = {};
    summary.v = 1;
    summary.id = id + constants.labels.summary;
    summary.metadata = metadata;
    summary.submittedAt = timestamp;
    summary.currentPage = 0;
    return summary;
}