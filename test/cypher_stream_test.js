var should     = require('should');
var cypher     = require('../index')('http://localhost:7474');

function shouldNotError(error) {
  should.not.exist(error);
}

describe('Cypher stream', function () {
  before(function (done){
    // Travis CI is slow.  Give him more time.
    if (process.env.TRAVIS_CI) {
      this.timeout(5000);
    }
    cypher('FOREACH (x IN range(1,10) | CREATE(:Test {test: true}))')
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
    cypher('match (n:Test) return n limit 10')
      .on('data', function (result) {
        results++;
        result.should.eql({ n: { test: true } });
      })
      .on('error', shouldNotError)
      .on('end', function() {
        results.should.eql(10);
        done();
      })
    ;
  });

  it('handles errors', function (done) {
    var errored = false;

    cypher('invalid query')
      .on('error', function (error) {
        errored = true;
        String(error).should.equal('Error: Query Failure: Invalid input \'i\': expected <init> (line 1, column 1)\n"invalid query"\n ^');
      })
      .on('end', function() {
        errored.should.be.true;
        done();
      })
      .resume() // need to manually start it since we have no on('data')
    ;
  });

  it('handles non-neo4j errors', function (done) {
    var errored       = false;
    var expectedError = new Error('Test');

    cypher('match (n:Test) return n limit 1')
      .on('data', function () {
        throw expectedError;
      })
      .on('error', function (error) {
        errored = true;
        error.should.equal(expectedError);
      })
      .on('end', function() {
        errored.should.be.true;
        done();
      })
    ;
  });

  it('returns non-object values', function (done) {
    cypher('match (n:Test) return n.test as test limit 1')
      .on('data', function (result) {
        result.should.eql({ test: true });
      })
      .on('error', shouldNotError)
      .on('end', done)
    ;
  });

  it('returns collections', function (done) {
    cypher('match (n:Test) return collect(n) as nodes limit 1')
      .on('data', function (result) {
        // 10x { test: true }
        result.should.eql({ nodes: [{ test: true }, { test: true }, { test: true }, { test: true }, { test: true }, { test: true }, { test: true }, { test: true }, { test: true }, { test: true } ] });
      })
      .on('error', shouldNotError)
      .on('end', done)
    ;
  });

  it('returns non-node collections', function (done) {
    cypher('match (n:Test) return labels(n) as labels limit 1')
      .on('data', function (result) {
        result.should.eql({ labels: ['Test']});
      })
      .on('error', shouldNotError)
      .on('end', done)
    ;
  });

  it('recursively returns data values', function (done) {
    cypher('match (n:Test) return { child: { grandchild: n }} as parent limit 1')
      .on('data', function (result) {
        result.should.eql({ parent: { child: { grandchild: { test: true } } } });
      })
      .on('error', shouldNotError)
      .on('end', done)
    ;
  });

  it('handles null', function (done) {
    cypher('return null')
      .on('data', function (result) {
        result.should.eql({ "null": null });
      })
      .on('error', shouldNotError)
      .on('end', done)
    ;
  });

  it('works with trailing slash', function (done){
    var cyp = require('../index')('http://localhost:7474/');
    cyp('match (n:Test) return n limit 1')
      .on('error', shouldNotError)
      .on('end', done)
      .resume()
    ;
  });

  it('works with basic http auth', function (done){
    var cyp = require('../index')('http://neo:cypher@localhost:7474/');
    cyp('match (n:Test) return n limit 1')
      .on('error', shouldNotError)
      .on('end', done)
      .resume()
    ;
  });

  describe('Transaction', function () {
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

    it('can do rollbacks', function () {
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
      transaction.write('match (n:Test) set n.foo = "bar" return n');
      transaction.write('match (n:Test) set n.bar = "baz" return n');
      transaction.rollback();
      cypher('match (n:Test) where n.foo = "bar" or n.bar = "baz" return count(n) as count')
        .on('data', function (results) {
          results.count.should.equal(0);
          done();
        })
      ;
    });

    it('handles transaction expiration', function () {
      // TODO
    });

  });

});

