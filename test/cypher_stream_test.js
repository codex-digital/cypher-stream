var should              = require('should');
var cypher              = require('../index')('http://localhost:7474');

function shouldNotError(error) {
  should.not.exist(error);
}

describe('Cypher stream', function () {
  before(function (done){
    this.timeout(5000); // sometimes travis ci takes too long here
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
      .on('data', function (result){
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
        String(error).should.equal('Error: Query failure: Invalid input \'i\': expected <init> (line 1, column 1)\n"invalid query"\n ^');
        error.neo4j.exception.should.equal('SyntaxException');
        error.neo4j.stacktrace.should.be.an.array;
        error.neo4j.statusCode.should.equal(400);
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
        error.thrown.should.equal(expectedError);
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

});
