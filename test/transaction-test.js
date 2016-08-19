'use strict';
var should = require('should');
var cypher = require('../index')('bolt://0.0.0.0');

function shouldNotError(error) {
  should.not.exist(error);
}

describe('Transaction', function () {

  var testRecordsToCreate = 10;
  before(function (done){
    // Travis CI is slow.  Give him more time.
    if (process.env.TRAVIS_CI) {
      this.timeout(5000);
    }
    cypher('FOREACH (x IN range(1,'+testRecordsToCreate+') | CREATE(:Test {test: true}))')
    .on('end', done)
    .on('error', shouldNotError)
    .resume();
  });

  after(done =>
    cypher('MATCH (n:Test) DELETE n')
    .on('end', done)
    .on('error', shouldNotError)
    .resume()
  );

  it('cannot write after commit (throw)', done => {
    var tx = cypher.transaction();
    tx.commit();
    try {
      tx.write('match (n:Test) return n limit 1');
    } catch (error) {
      should.equal(
        'Error: Cannot write after commit.',
        String(error)
      );
    }
    tx.on('end', done);
    tx.resume();
  });

  it('cannot write after commit (emit)', done => {
    var tx = cypher.transaction();
    tx.commit();
    tx.on('error', error =>
      should.equal(
        'Error: Cannot write after commit.',
        String(error)
      )
    );
    tx.on('end', done);
    tx.resume();
  });

  it('works', function (done) {
    var results = 0;
    var transaction = cypher.transaction()
    .on('data', result => {
      results++;
      result.should.eql({ n: { test: true } });
    })
    .on('error', shouldNotError)
    .on('end', function() {
      results.should.eql(1);
      done();
    })
    ;
    transaction.write('match (n:Test) return n limit 1');
    transaction.commit();
  });

  context('data written within a transaction', () => {

    var tx;
    beforeEach(() => {
      tx = cypher.transaction();
      tx.write('create (n:NewItem { foo: "bar"}) return n');
    });
    afterEach(done =>
      cypher(`
        MATCH (n:NewItem)
        DETACH DELETE n
      `)
      .on('end', done)
      .resume()
    );

    it('is unavailable to parallel transactions', done => {
      var tx2    = cypher.transaction();
      var called = false;
      tx.on('data', () => {
        tx2.write('MATCH (n:NewItem) return n');
        tx2.on('data', () => called = true);
        tx2.on('end',  () => {
          should.equal(false, called);
          tx.rollback();
        });
        tx2.commit();
      });

      tx.on('end', done);

      tx2.resume();
      tx.resume();

    });

    // TODO: look into this.
    it.skip('Uncaught Read operations are not allowed for `NONE` transactions', done => {
      var tx2    = cypher.transaction();
      var called = false;

      tx.on('data', () => {
        console.log('here');
        tx2.write('MATCH (n:NewItem) return n');
        tx2.on('data', () => called = true);
        tx2.on('end',  () => should.equal(false, called));
      });

      tx2.on('end', done);

      tx2.resume();
      tx2.commit();
      tx.resume();
      // tx.commit();
    });

    it('is available after commit', done => {
      var tx = cypher.transaction();
      var numToCreate = 10;
      for(var i = 0; i < numToCreate; i++) {
        tx.write('create (n:NewItem { foo: "bar"}) return n');
      }

      var numResults = 0;

      tx.on('end', () => {
        cypher('match (n:NewItem) return n')
        .on('data', data => {
          should.equal('bar', data.n.foo);
          numResults++;
        })
        .on('end', () => {
          should.equal(numToCreate, numResults);
          done();
        });
      });

      tx.commit();
      tx.resume();

    });

    it('is unavailable outside the transaction before commit', done => {
      var tx = cypher.transaction();
      tx.write('create (n:NewItem { foo: "bar"}) return n');

      tx.on('data', () => {
        var called = false;
        cypher('match (n:NewItem) return n')
        .on('data', () => called = true)
        .on('end',  () => {
          should.equal(false, called);
          tx.commit();
        });
      });

      tx.on('end', done);
      tx.resume();
    });

  });

  it('handles multiple writes', function (done) {
    var results = 0;
    var transaction = cypher.transaction()
    .on('data', function (result) {
      results++;
      result.should.eql({ n: { test: true } });
    })
    .on('error', shouldNotError)
    .on('end', function() {
      results.should.eql(2);
      done();
    })
    ;
    transaction.write('match (n:Test) return n limit 1');
    transaction.write('match (n:Test) return n limit 1');
    transaction.commit();
  });

  it('accepts a variety of statement formats', function (done) {
    var results = 0;
    var transaction = cypher.transaction()
    .on('data', function (result) {
      results++;
      result.should.eql({ n: { test: true } });
    })
    .on('error', shouldNotError)
    .on('end', function() {
      results.should.eql(6);
      done();
    })
    ;
    var query = 'match (n:Test) return n limit 1';
    transaction.write(query);
    transaction.write({ statement: query });
    transaction.write([ query, query ]);
    transaction.write([ { statement: query }, { statement: query } ]);
    transaction.commit();
  });

  it('handles write and commit', function (done) {
    var results = 0;
    var transaction = cypher.transaction()
    .on('data', function (result) {
      results++;
      result.should.eql({ n: { test: true } });
    })
    .on('error', shouldNotError)
    .on('end', function() {
      results.should.eql(1);
      done();
    })
    ;
    transaction.write({ statement: 'match (n:Test) return n limit 1', commit: true });
  });

  it('can eagerly rollback if queries are still buffered', function (done) {
    var results = 0;
    var transaction = cypher.transaction()
    .on('data', function (result) {
      results++;
      result.should.eql({ n: { test: true } });
    })
    .on('error', shouldNotError)
    .on('end', function () {
      results.should.eql(0);
      cypher('match (n:Test) where n.foo = "bar" or n.bar = "baz" return count(n) as count')
        .on('data', result => {
          result.count.should.equal(0);
        })
        .on('end', done)
        .on('error', shouldNotError)
      ;
    })
    ;
    transaction.write('match (n:Test) set n.foo = "bar" return n');
    transaction.write('match (n:Test) set n.bar = "baz" return n');
    transaction.rollback();
  });

  it('can rollback even if the queries have had time to send', function (done) {
    var results = 0;
    var timeout = process.env.TRAVIS_CI ? 1000 : 50;
    var transaction = cypher.transaction()
    .on('data', function () {
      results++;
    })
    .on('error', shouldNotError)
    .on('end', function () {
      results.should.eql(testRecordsToCreate*2);
      cypher('match (n:Test) where n.foo = "bar" or n.bar = "baz" return count(n) as count')
        .on('data', function (result) {
          result.count.should.equal(0);
          done();
        })
        .on('error', shouldNotError)
      ;
    })
    ;
    transaction.write('match (n:Test) set n.foo = "bar" return n');
    transaction.write('match (n:Test) set n.bar = "baz" return n');
    setTimeout(function () {
      transaction.rollback();
    }, timeout);
  });

  it('works with parameters', function (done) {
    var results = 0;
    var transaction = cypher.transaction()
    .on('data', function (result) {
      results++;
      result.should.eql({ n: { test: true } });
    })
    .on('error', shouldNotError)
    .on('end', function () {
      results.should.eql(1);
      done();
    })
    ;
    transaction.write({
      statement  : 'match (n:Test) where n.test={test} return n limit 1',
      parameters : { test: true },
      commit     : true,
    });
  });

  it('calls statement callbacks', function (done) {
    var results = 0;
    var calls   = 0;
    var ended   = 0;
    var query   = 'match (n:Test) return n limit 2';
    function callback(stream) {
      stream
      .on('data', function (result) {
        result.should.eql({ n: { test: true } });
        results++;
      })
      .on('end', function () {
        ended++;
      })
      ;
      calls++;
    }
    var statement = { statement: query, callback: callback };
    var transaction = cypher.transaction();
    transaction.write(statement);
    transaction.write(statement);
    transaction.commit();
    transaction.resume();
    transaction.on('end', function() {
      calls.should.equal(2);
      ended.should.equal(2);
      results.should.equal(4);
      done();
    });
  });

  it('can return Neo4j data types', done => {
    var tx = cypher.transaction({ returnType: 'neo4j' });

    tx.on('data', data => {
      data.should.have.properties([
        '_fields',
        'keys',
        'length',
        '_fieldLookup',
      ]);
    });

    tx.write('MATCH (n:Test) return n LIMIT 1');
    tx.commit();
    tx.resume();
    tx.on('end', done);
  });

});
