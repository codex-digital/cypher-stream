'use strict';
var Duplex       = require('stream').Duplex;
var util         = require('util');
var CypherStream = require('./CypherStream');
var normalize    = require('./normalize-query-statement');

util.inherits(TransactionStream, Duplex);

// Options:
// - debounceTime: number of milliseconds to wait between queries to collect and
//   batch request them.
// - batchSize: maximimum number of queries to send at a time.
// - metadata: true if node & relationship metadata should be returned too,
//   not just property data. (This translates to Neo4j's REST format.)
function TransactionStream(url, options) {
  Duplex.call(this, { objectMode: true });

  var self = this;
  var buffer = [];
  var transactionId;
  var debounce;
  var debounceTime = options && options.debounceTime || 0;
  var batchSize    = options && options.batchSize || 10000;
  var metadata     = options && options.metadata;

  this.commit = function commitAlias() {
    return self.write({ commit: true });
  };

  this.rollback = function rollbackAlias() {
    return self.write({ rollback: true });
  };

  function handle() {
    var statements = [];
    var callbacks  = [];
    var options    = {};

    if(metadata) {
      options.metadata = metadata;
    }

    if (transactionId) {
      options.transactionId = transactionId;
    }
    while(buffer.length) {
      var input = buffer.shift();
      statements = statements.concat(normalize(input));
      if (input.commit)   { options.commit   = true; }
      if (input.rollback) { options.rollback = true; }
    }

    // console.log('new CypherStream', url, statements, options);
    var stream = new CypherStream(url, statements, options);

    stream.on('transactionId', function transactionIdHandler(txId) {
      if (!transactionId) {
        transactionId = txId;
      }
    });

    stream.on('expires', function expiresHandler(date) {
      self.emit('expires', date);
    });

    stream.on('transactionExpired', function transactionExpiredHandler() {
      self.emit('expired');
    });

    stream.on('data', function dataHandler(data) {
      self.push(data);
    });

    stream.on('error', function errorHandler(errors) {
      self.emit('error', errors);
    });

    stream.on('end', function endHandler() {
      if(options.rollback || options.commit) {
        self.push(null);
      }
    });

  }

  this._write = function (chunk, encoding, callback) {
    buffer.push(chunk);
    if(debounce) { clearTimeout(debounce); }
    // debounce to allow writes to buffer
    if(buffer.length === batchSize) {
      handle();
    } else {
      debounce = setTimeout(handle, debounceTime);
    }
    callback();
  };
  this._read = function () { };

}

module.exports = TransactionStream;
