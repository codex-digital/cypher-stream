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

  it('handles transaction expiration', function () {
    // TODO
  });

});
