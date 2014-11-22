'use strict';
var Duplex       = require('stream').Duplex;
var util         = require('util');
var CypherStream = require('./CypherStream');
var normalize    = require('./normalize-query-statement');

util.inherits(TransactionStream, Duplex);

function TransactionStream(url, options) {
  Duplex.call(this, { objectMode: true });

  var self = this;
  var transactionId;
  // time to wait between queries to collect and batch request them
  var debounceTime = options && options.debounceTime || 0;
  // maximimum number of queries to send at a time
  var batchSize    = options && options.batchSize || 10000;

  this.commit = function () {
    return self.write({ commit: true });
  };

  this.rollback = function () {
    return self.write({ rollback: true });
  };

  function processChunk(input, encoding, callback) {
    var statements = normalize(input);
    var callbacks  = [callback];
    var options    = {};
    if (input.commit) {
      options.commit = true;
    }
    if (input.rollback) {
      options.rollback = true;
    }
    if (transactionId) {
      options.transactionId = transactionId;
    }
    // combine any buffered queries
    var buffer = self._writableState.buffer;
    while (buffer.length && statements.length < batchSize && !options.rollback) {
      var buffered = buffer.shift();
      var bufferedStatements = normalize(buffered.chunk);
      if (bufferedStatements) {
        statements = statements.concat(bufferedStatements);
      }
      if (buffered.chunk.commit) {
        options.commit = true;
      }
      if (buffered.chunk.rollback) {
        options.rollback = true;
      }
      callbacks.push(buffered.callback);
    }

    var stream = new CypherStream(url, statements, options);

    stream.on('transactionId', function (txId) {
      if (!transactionId) {
        transactionId = txId;
      }
    });

    stream.on('expires', function (date) {
      self.emit('expires', date);
    });

    stream.on('transactionExpired', function () {
      self.emit('expired');
    });

    stream.on('data', function (data) {
      self.push(data);
    });

    stream.on('error', function (errors) {
      self.emit('error', errors);
    });

    stream.on('end', function () {
      callbacks.forEach(function (callback) {
        callback();
      });
      if(options.rollback || options.commit) {
        self.push(null);
      }
    });

  }

  this._write = function (input, encoding, done) {
    setTimeout(function () {
      processChunk(input, encoding, done);
    }, debounceTime);
  };
  this._read = function () { };

}

module.exports = TransactionStream;
