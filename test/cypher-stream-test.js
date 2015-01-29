'use strict';
var should     = require('should');
var cypher     = require('../index')('http://localhost:7474');
var http       = require('http');

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

  it('supports node/rel metadata', function (done) {
    var results = 0;
    cypher({
      statement: 'match (n:Test) return n limit 1',
      metadata: true
    })
      .on('data', function (result) {
        results++;
        result.should.be.type('object');
        result.n.should.be.type('object');
        result.n.data.should.eql({ test: true });
        result.n.self.should.be.type('string');
        result.n.metadata.should.be.type('object');
        result.n.metadata.id.should.be.type('number');
        result.n.metadata.labels.should.eql(['Test']);
      })
      .on('error', shouldNotError)
      .on('end', function () {
        results.should.eql(1);
        done();
      })
    ;
  });

  it('supports custom headers', function (done) {
    var headers = {
      'X-Foo': 'Bar',
      'x-lorem': 'ipsum'
    };

    // for this test, use a mock server to test that headers were actually sent,
    // but then proxy the request to Neo4j afterward.
    // TODO: use https://github.com/pgte/nock? not sure how to check request
    // headers though (not just match them).
    var server = http.createServer(function (req, res) {
      req.headers['x-foo'].should.equal('Bar');
      req.headers['x-lorem'].should.equal('ipsum');

      // proxy request to real Neo4j server:
      req.pipe(http.request({
        hostname: 'localhost',
        port: 7474,
        method: req.method,
        path: req.url,
        headers: req.headers
      }, function (neo4jRes) {
        res.writeHead(neo4jRes.statusCode, neo4jRes.headers);
        neo4jRes.pipe(res);
      }));

      // and shut down the server now (so Node can cleanly exit):
      server.close();
    });

    server.listen(0, function () {
      var url = 'http://localhost:' + server.address().port;
      var cypher = require('../')(url);

      var results = 0;
      cypher({
        statement: 'match (n:Test) return n limit 1',
        headers: headers
      })
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
  });

});
