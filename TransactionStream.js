'use strict';
var $                  = require('highland');
var CypherStream       = require('./CypherStream');
var Duplex             = require('stream').Duplex;
var normalize          = require('./util/normalize-query-statement');
// var R                  = require('ramda');

// var tap = R.tap;
// var log = tap(console.log.bind(console));

class TransactionStream extends Duplex {
  constructor(session, options) {
    super({ objectMode: true });

    this.session    = session;
    this.tx         = session.beginTransaction();
    this.statements = $();

    this.writes = this.statements.fork()
    .flatMap(normalize)
    .map(statement => {
      if(statement.commit) {
        this.commit();
      }
      return $(new CypherStream(this.tx, statement, options));
    })
    ;

    this.results = this.writes.fork()
    .flatten()
    .doto(x => this.push(x))
    .errors(error => this.emit('error', error))
    ;

    this.writes.resume();
    this.results.resume();
  }

  _write(chunk, encoding, callback) {
    if(this.rolledBack) {
      throw new Error('Cannot write after rollback.');
    }
    if(this.committed) {
      throw new Error('Cannot write after commit.');
    }
    this.statements.write(chunk);
    callback();
  }

  _read() { }

  commit() {
    if(this.committed) {
      return;
    }
    this.committed = true;

    this.writes.on('end', () => {
      this.tx.commit()
      .subscribe({
        onCompleted: () => {
          this.emit('comitted');
          this.push(null);
        },
        onError: error => {
          this.emit('error', error);
          this.push(null);
        }
      });
    });

    this.statements.end();
  }

  rollback() {
    if(this.rolledBack) {
      return;
    }
    this.rolledBack = true;

    this.statements.end();
    this.results.end();
    this.writes.end();

    this.tx.rollback()
    .subscribe({
      onCompleted: () => {
        this.push(null);
      },
      onError: error => {
        this.emit('error', error);
        this.push(null);
      }
    });

  }

}

module.exports = TransactionStream;
