'use strict';
var cypher = require('../index')('bolt://0.0.0.0');
var neo4j  = require('neo4j-driver').v1;
var R      = require('ramda');
var should = require('should');

var shouldNotError = error => should.not.exist(error);

describe('Cypher stream', () => {

  var testRecordsToCreate = 10;
  before(function(done) {
    // Travis CI is slow.  Give more time.
    if (process.env.TRAVIS_CI) {
      this.timeout(5000);
    }
    cypher('FOREACH (x IN range(1,'+testRecordsToCreate+') | CREATE(:Test {test: true}))')
    .on('end', done)
    .on('error', shouldNotError)
    .resume();
  });
  after(done => {
    cypher('MATCH (n:Test) DELETE n')
    .on('end', done)
    .on('error', shouldNotError)
    .resume();
  });



  it('exposes base Neo4j Node and Relationship for external comparisons', () => {
    cypher.neo4j.types.should.have.properties('Node', 'Relationship');
  });

  it('exposes to-native', () => {
    // this function could be useful for client wrappers wanting to implement
    // mapping middleware of their own, but delegate back to native in certain cases
    cypher.toNative.should.be.a.Function();
  });

  describe('numbers', () => {

    it('returns integer for max safe', done =>
      cypher(`return ${Number.MAX_SAFE_INTEGER} as number`)
      .on('data', result => {
        result.number.should.eql(Number.MAX_SAFE_INTEGER);
      })
      .on('error', shouldNotError)
      .on('end', done)
    );

    it('returns integer for min safe', done =>
      cypher(`return ${Number.MIN_SAFE_INTEGER} as number`)
      .on('data', result => {
        result.number.should.be.a.Number();
        result.number.should.eql(Number.MIN_SAFE_INTEGER);
      })
      .on('error', shouldNotError)
      .on('end', done)
    );

    it('returns strings for > max safe', done =>
      cypher(`return ${Number.MAX_SAFE_INTEGER+1} as number`)
      .on('data', result => {
        result.number.should.be.a.String();
        result.number.should.eql(String(Number.MAX_SAFE_INTEGER+1))
      })
      .on('error', shouldNotError)
      .on('end', done)
    );

    it('returns strings for < min safe', done =>
      cypher(`return ${Number.MIN_SAFE_INTEGER-1} as number`)
      .on('data', result => {
        result.number.should.be.a.String();
        result.number.should.eql(String(Number.MIN_SAFE_INTEGER-1))
      })
      .on('error', shouldNotError)
      .on('end', done)
    );

  });


  it('works', done => {
    var results = 0;
    cypher('match (n:Test) return n limit 10')
    .on('data', result => {
      results++;
      result.should.eql({ n: { test: true } });
    })
    .on('error', shouldNotError)
    .on('end', () => {
      results.should.eql(10);
      done();
    })
    ;
  });

  it('handles errors', done => {
    var errored = false;

    cypher('invalid query')
    .on('error', error => {
      errored = true;
      should.equal(
        error.code,
        'Neo.ClientError.Statement.SyntaxError'
      );
      should.equal(
        error.message,
        'Invalid input \'i\': expected <init> (line 1, column 1 (offset: 0))\n"invalid query"\n ^'
      );
    })
    .on('end', () => {
      should.equal(true, errored);
      done();
    })
    .resume()
    ;
  });

  it('returns non-object values', done => {
    cypher('match (n:Test) return n.test as test limit 1')
    .on('data', result => result.should.eql({ test: true }))
    .on('error', shouldNotError)
    .on('end', done)
    ;
  });

  it('returns collections', done => {
    cypher('match (n:Test) return collect(n) as nodes limit 1')
    .on('data', result => {
      result.should.eql({
        nodes: R.times(R.always({ test: true }), 10)
      });
    })
    .on('error', shouldNotError)
    .on('end', done)
    ;
  });

  it('returns non-node collections', done => {
    cypher('match (n:Test) return labels(n) as labels limit 1')
    .on('data', result => result.should.eql({ labels: ['Test']}))
    .on('error', shouldNotError)
    .on('end', done)
    ;
  });

  it('returns relationships', done => {
    cypher(`
      CREATE ()-[a:somerel { foo: 'bar' }]->()
      RETURN a
    `)
    .on('data', result => result.should.eql({ a: { foo: 'bar' } }))
    .on('error', shouldNotError)
    .on('end', done)
    ;
  });

  it('recursively returns data values', done => {
    cypher('match (n:Test) return { child: { grandchild: n }} as parent limit 1')
    .on('data', result => result.should.eql({ parent: { child: { grandchild: { test: true } } } }))
    .on('error', shouldNotError)
    .on('end', done)
    ;
  });

  it('handles null', done => {
    cypher('return null')
    .on('data', result => result.should.eql({ 'null': null }))
    .on('error', shouldNotError)
    .on('end', done)
    ;
  });

  it('works with parameters', done => {
    var results = 0;
    cypher('match (n:Test) where n.test={test} return n limit 1', { test: true })
    .on('data', result => {
      results++;
      result.should.eql({ n: { test: true } });
    })
    .on('error', shouldNotError)
    .on('end', () => {
      results.should.eql(1);
      done();
    })
    ;
  });

  it('handles accepts a variety of statement formats', done => {
    var results = 0;
    var query   = 'match (n:Test) return n limit 1';
    [
      cypher(query),
      cypher({ statement: query }),
      cypher([ query, query ]),
      cypher([ { statement: query }, { statement: query } ]),
    ].forEach(stream => {
      stream.on('data', result => {
        results++;
        result.should.eql({ n: { test: true } });
        if(results === 6) {
          done();
        }
      });
    });
  });

  it('calls statement callbacks', done => {
    var results = 0;
    var calls   = 0;
    var ended   = 0;
    var query   = 'match (n:Test) return n limit 2';
    var callback = stream => {
      stream
      .on('data', result => {
        result.should.eql({ n: { test: true } });
        results++;
      })
      .on('end', () => {
        ended++;
      })
      ;
      calls++;
    };
    var statement = { statement: query, callback: callback };
    cypher([ statement, statement ])
    .on('end', () => {
      calls.should.equal(2);
      ended.should.equal(2);
      results.should.equal(4);
      done();
    })
    .resume();
  });

  it('can return Neo4j data types', done => {
    cypher(`match (n:Test) return n limit 1`, {}, { returnType: 'neo4j' })
    .on('data', data => {
      data.should.have.properties([
        '_fields',
        'keys',
        'length',
        '_fieldLookup',
      ]);
    })
    .on('end', done)
    ;
  });

  it('converts properties of Nodes to native', done => {
    cypher('create (n { foo: 1 }) return n ')
    .on('data', result => result.should.eql({
      n: { foo: 1 }
    }))
    .on('error', shouldNotError)
    .on('end', done)
    ;
  });
});
