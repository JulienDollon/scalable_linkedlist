'use strict';

const chai = require('chai');
const expect = chai.expect;
const scalableLinkedList = require('scalable_linked_list');

const uuid = require('uuid');

const currentLinkedListId = uuid.v1();
const region = 'eu-west-3';
const table = 'testlinkedlist';
const maxEltPerPage = 2;

describe('scalable_linked_list', function() {
    it('configuring the linkedlist should work successfully', function() {
        scalableLinkedList.configureDynamoDB(region, table);
        scalableLinkedList.configureMaximumNumberOfElementPerPage(maxEltPerPage);

        expect(scalableLinkedList.getCurrentConfiguration().region).to.equal(region);
        expect(scalableLinkedList.getCurrentConfiguration().tableName).to.equal(table);
        expect(scalableLinkedList.getCurrentConfiguration().maxElementPerPage).to.equal(maxEltPerPage);
    });

    it('It can create a summary page successfully', function(done) {
        this.timeout(15000);

        const checkIfSummaryIdIsValid = function(summaryObject) {
            expect(summaryObject.id).to.equal(currentLinkedListId + scalableLinkedList.getConstants().labels.summary);
            return summaryObject;
        };
        const getPageDataFromDB = function(summaryObject) {
            return scalableLinkedList.getPage(currentLinkedListId, scalableLinkedList.getConstants().labels.summary);
        };
        const complete = function() { done(); }

        scalableLinkedList.idempotentCreate(currentLinkedListId)
            .then(checkIfSummaryIdIsValid)
            .then(getPageDataFromDB)
            .then(checkIfSummaryIdIsValid)
            .then(complete);
    });

    it('It sets currentPage at 0 correctly', function(done) {
        this.timeout(15000);
        scalableLinkedList.getCurrentPage(currentLinkedListId)
        .then(function(currentPage) {
            expect(currentPage).to.equal(0);
            done();
        });
    });

    it('It can append messages successfully and scale up', function(done) {
        this.timeout(25000);
        var i = 0;
        var atomicAppendCount = 0;
        
        const asyncAppendCalls = function(numberOfCallsToMake, completedCallback, currentCall = 0) {
            console.log(currentCall + ' ' + numberOfCallsToMake);
            if (currentCall >= numberOfCallsToMake) {
                completedCallback();
                return;
            }
            scalableLinkedList.atomicAppend(currentLinkedListId, { val: 'Hello' + currentCall }).then(function(success) {
                asyncAppendCalls(numberOfCallsToMake, completedCallback, currentCall + 1);
            });
        }

        asyncAppendCalls(5, function() {
            scalableLinkedList.getCurrentPage(currentLinkedListId).then(function(currentPage) {
                expect(currentPage).to.equal(2);
                done();
            });
        });
    });

    it('It can retrieve the last element', function(done) {
        this.timeout(15000);
        scalableLinkedList.getCurrentPage(currentLinkedListId).then(function(currentPage) {
            scalableLinkedList.retrieve(currentLinkedListId, currentPage).then(function(data_list) {
                expect(data_list.data[0].val).to.equal('Hello4');
                expect(data_list.data[0].page_id).to.equal(currentPage + '');
                expect(data_list.data[0].sequence_id).to.equal('0');
                done();
            });
        });
    });

    it('It can retrieve the last element with retrieveLastMostRecent', function(done) {
        this.timeout(15000);
        scalableLinkedList.retrieveLastMostRecent(currentLinkedListId, 1, function(result) {
            expect(result.length).to.equal(1);
            expect(result[0].val).to.equal('Hello4');
            done();
        });
    });

    it('It can retrieve the last N elements with retrieveLastMostRecent accross pages', function(done) {
        this.timeout(15000);
        scalableLinkedList.retrieveLastMostRecent(currentLinkedListId, 3, function(result) {
            expect(result.length).to.equal(3);
            expect(result[0].val).to.equal('Hello4');
            expect(result[1].val).to.equal('Hello3');
            expect(result[2].val).to.equal('Hello2');
            done();
        });
    });

    it('It can retrieve the last N elements with N too big', function(done) {
        this.timeout(15000);
        scalableLinkedList.retrieveLastMostRecent(currentLinkedListId, 300, function(result) {
            expect(result.length).to.equal(5);

            expect(result[0].val).to.equal('Hello4');
            expect(result[0].page_id).to.equal('2');
            expect(result[0].sequence_id).to.equal('0');

            expect(result[1].val).to.equal('Hello3');
            expect(result[1].page_id).to.equal('1');
            expect(result[1].sequence_id).to.equal('1');

            expect(result[2].val).to.equal('Hello2');
            expect(result[2].page_id).to.equal('1');
            expect(result[2].sequence_id).to.equal('0');

            expect(result[3].val).to.equal('Hello1');
            expect(result[3].page_id).to.equal('0');
            expect(result[3].sequence_id).to.equal('1');

            expect(result[4].val).to.equal('Hello0');
            expect(result[4].page_id).to.equal('0');
            expect(result[4].sequence_id).to.equal('0');

            done();
        });
    });

    it('retrieveNextMostRecent can retrieve the next N elements with N too big', function(done) {
        this.timeout(15000);
        scalableLinkedList.retrieveLastMostRecent(currentLinkedListId, 1, function(result) {
            expect(result.length).to.equal(1);

            expect(result[0].val).to.equal('Hello4');
            expect(result[0].page_id).to.equal('2');
            expect(result[0].sequence_id).to.equal('0');

            scalableLinkedList.retrieveNextMostRecent(currentLinkedListId,
                                                      {
                                                          page_id: result[0].page_id,
                                                          sequence_id: result[0].sequence_id
                                                      },
                                                      300,
                function(result2) {
                    expect(result2[0].val).to.equal('Hello3');
                    expect(result2[0].page_id).to.equal('1');
                    expect(result2[0].sequence_id).to.equal('1');

                    expect(result2[1].val).to.equal('Hello2');
                    expect(result2[1].page_id).to.equal('1');
                    expect(result2[1].sequence_id).to.equal('0');

                    expect(result2[2].val).to.equal('Hello1');
                    expect(result2[2].page_id).to.equal('0');
                    expect(result2[2].sequence_id).to.equal('1');

                    expect(result2[3].val).to.equal('Hello0');
                    expect(result2[3].page_id).to.equal('0');
                    expect(result2[3].sequence_id).to.equal('0');

                    done();
            });
        });
    });
});