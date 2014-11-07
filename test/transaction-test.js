var should     = require('should');
var cypher     = require('../index')('http://localhost:7474');

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
  after(function (done){
    cypher('MATCH (n:Test) DELETE n')
      .on('end', done)
      .on('error', shouldNotError)
      .resume();
  });

  it('works', function (done) {
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
    transaction.write('match (n:Test) return n limit 1');
    transaction.commit();
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

  it('handles accepts a variety of statement formats', function (done) {
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

  it('automatically batches queries for performance', function (done) {
    // results may vary, depending on your system.
    // tests on macbook pro were around ~100ms
    // Travis CI is slow.  Give him more time.
    if (process.env.TRAVIS_CI) {
      this.timeout(5000);
    }
    var results = 0;
    var queriesToRun = 1000;
    var queriesWritten = 0;
    var transaction = cypher.transaction()
      .on('data', function (result) {
        results++;
        result.should.eql({ n: { test: true } });
      })
      .on('error', shouldNotError)
      .on('end', function() {
        results.should.eql(queriesToRun);
        done();
      })
    ;
    while (queriesWritten++ < queriesToRun) {
      transaction.write('match (n:Test) return n limit 1');
    }
    transaction.commit();
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
    transaction.rollback();
  });

  it('can rollback even if the queries have had time to send', function (done) {
    var results = 0;
    var transaction = cypher.transaction()
      .on('data', function (result) {
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
    }, 50);
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

  it('emits expiration', function (done) {
    var called = false;
    var transaction = cypher.transaction()
      .on('error', shouldNotError)
      .on('expires', function (date) {
        called = true;
      })
      .on('end', function () {
        called.should.equal(true);
        done();
      })
    ;

    transaction.resume();

    transaction.write('match (n:Test) return n limit 1');

    setTimeout(function() {
      transaction.write('match (n:Test) return n limit 1');
      transaction.commit();
    }, 0);

  });

  it.skip('handles expiration', function (done) {
    var serverTimeout = 60; // set this equal to neo4j-server.properties -> org.neo4j.server.transaction.timeout (default 60)
    var errorCalled   = false;
    var expiresCalled = false;
    var expiredCalled = false;
    var transaction   = cypher.transaction();

    this.timeout((serverTimeout+10)*1000);

    transaction.on('expires', function (date) {
      expiresCalled = true;
    });

    transaction.on('expired', function () {
      expiredCalled = true;
    });

    transaction.on('error', function (error) {
      errorCalled   = true;
      error.neo4j.should.eql({
        statusCode: 400,
        errors: [{
          code    : 'Neo.ClientError.Transaction.UnknownId',
          message : 'Unrecognized transaction id. Transaction may have timed out and been rolled back.'
        }],
        results: []
      });
    });

    transaction.on('end', function () {
      errorCalled  .should.equal(true);
      expiresCalled.should.equal(true);
      expiredCalled.should.equal(true);
      done();
    });

    transaction.resume();
    transaction.write('match (n:Test) return n limit 1');

    setTimeout(function() {
      transaction.write('match (n:Test) return n limit 1');
      transaction.commit();
    }, ((serverTimeout+5)*1000));
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

});
