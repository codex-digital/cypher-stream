'use strict';
var should     = require('should');
var cypher     = require('../index')('http://localhost:7474');

function shouldNotError(error) {
  should.not.exist(error);
}

describe('Cypher stream', function () {
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
    cypher('match (n:Test) return n limit 10')
      .on('data', function (result) {
        results++;
        result.should.eql({ n: { test: true } });
      })
      .on('error', shouldNotError)
      .on('end', function () {
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
      .on('end', function () {
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
      .on('end', function () {
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
        result.should.eql({ 'null': null });
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

  it('works with parameters', function (done) {
    var results = 0;
    cypher('match (n:Test) where n.test={test} return n limit 1', { test: true })
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
  });

  it('handles accepts a variety of statement formats', function (done) {
    var results = 0;
    var query   = 'match (n:Test) return n limit 1';
    [
      cypher(query),
      cypher({ statement: query }),
      cypher([ query, query ]),
      cypher([ { statement: query }, { statement: query } ]),
    ].forEach(function (stream) {
      stream.on('data', function (result) {
        results++;
        result.should.eql({ n: { test: true } });
        if(results === 6) {
          done();
        }
      });
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
    cypher([ statement, statement ]).on('end', function () {
      calls.should.equal(2);
      ended.should.equal(2);
      results.should.equal(4);
      done();
    }).resume();
  });

});

