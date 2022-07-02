'use strict';

const scalableLinkedList = require('scalable_linked_list');
const uuid = require('uuid');

describe('Documentation', function() {
    it('This is how to use the scalable_linked_list!', function() {

        const myLinkedList = uuid.v1();
        const initializeFramework = function() {
            scalableLinkedList.configureDynamoDB('eu-west-3', 'testlinkedlist');
            scalableLinkedList.configureMaximumNumberOfElementPerPage(5);
        };

        const createLinkedList = function() {
            return scalableLinkedList.idempotentCreate(myLinkedList);
        }

        const insertValue = function() {
            const valueToInsert = 'My value!';
            return scalableLinkedList.atomicAppend(myLinkedList, valueToInsert)
        }

        const retrieveValue = function() {
            scalableLinkedList.retrieveLastMostRecent(myLinkedList, 1, function(data) {
                console.log('This is your data: ' + data[0]);
            });
        }

        initializeFramework();
        createLinkedList()
        .then(insertValue)
        .then(retrieveValue);
    });
});