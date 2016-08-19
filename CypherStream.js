'use strict';
var $                  = require('highland');
var neo4j              = require('neo4j-driver').v1;
var normalize          = require('./util/normalize-query-statement');
var observableToStream = require('./util/observable-to-stream');
var R                  = require('ramda');
var Readable           = require('stream').Readable;
var toNative           = require('./util/to-native');

var compose       = R.compose;
var cond          = R.cond;
var curry         = R.curry;
var has           = R.has;
var map           = R.map;
var prop          = R.prop;

// var tap = R.tap;
// var log = tap(console.log.bind(console));

// session => statement => observable
var run = curry((runner, statement) =>
  runner.run(statement.statement, statement.parameters)
);

var runStream = curry(compose(observableToStream, run));

var emitError = R.curry((stream, error) =>
  stream.emit('error', error)
);

var handleNeo4jError = emit => compose(
  map(compose(
    emit,
    error => new neo4j.Neo4jError(error.message, error.code)
  )),
  prop('fields')
);

var handleError = emit => cond([
  [isNeo4jError, handleNeo4jError(emit)],
  [R.T, emit]
]);

var isNeo4jError = R.has('fields');

class CypherStream extends Readable {

  constructor(runner, statements, options) {
    super({ objectMode: true });
    this.statements = statements;
    this.runner     = runner;
    this.options    = options || {};
    this.start();
  }

  start() {
    $([this.statements])
    .flatMap(normalize)
    .filter(has('statement'))
    .flatMap(statement => {

      var stream = runStream(this.runner, statement);

      if('neo4j' !== this.options.returnType) {
        stream = stream.map(toNative);
      }

      if(statement.callback) {
        statement.callback(stream.observe());
      }

      return stream;

    })
    .errors(handleError(emitError(this)))
    .doto(x => this.push(x))
    .on('end', () => this.push(null))
    .resume()
    ;
  }

  _read() {}
}

module.exports = CypherStream;
