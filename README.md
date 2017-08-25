# cypher-stream
[![Build Status](https://travis-ci.org/codex-digital/cypher-stream.svg?branch=master)](https://travis-ci.org/codex-digital/cypher-stream)
[![devDependency Status](https://david-dm.org/codex-digital/cypher-stream.png?theme=shields.io)](https://david-dm.org/codex-digital/cypher-stream.png#info=devDependencies)
[![NPM version](https://badge.fury.io/js/cypher-stream.png)](http://badge.fury.io/js/cypher-stream)
[![Coverage Status](https://coveralls.io/repos/github/codex-digital/cypher-stream/badge.svg?branch=master)](https://coveralls.io/github/codex-digital/cypher-stream?branch=master)
[![Slack Status](https://codex-community-slackin.herokuapp.com/badge.svg)](https://codex-community-slackin.herokuapp.com)

Neo4j cypher queries as node object streams.

## Installation
```
npm install cypher-stream
```

## Basic usage

``` js
var cypher = require('cypher-stream')('bolt://localhost', 'username', 'password');

cypher('match (user:User) return user')
  .on('data', function (result){
    console.log(result.user.first_name);
  })
  .on('end', function() {
    console.log('all done');
  })
;
```

## Handling errors
``` js
var cypher = require('cypher-stream')('bolt://localhost', 'username', 'password');
var should = require('should');
it('handles errors', function (done) {
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

```

## Transactions


Transactions are duplex streams that allow you to write query statements and read the results.

Transactions have three methods: `write`, `commit`, and `rollback`, which add queries and commit or rollback the queue respectively.

### Creating a transaction

``` js
var transaction = cypher.transaction(options)
```

### Adding queries to a transaction

``` js
transaction.write(query_statement);
```

A `query_statement` can be a string or a query statement object.  A query statement object consists of a `statement` property and an optional `parameters` property.  Additionally, you can pass an array of either.

The following are all valid options:

``` js
var transaction = cypher.transaction();

transaction.write('match (n:User) return n');

transaction.write({ statement: 'match (n:User) return n' });

transaction.write({
  statement  : 'match (n:User) where n.first_name = {first_name} return n',
  parameters : { first_name: "Bob" }
});

transaction.write([
  {
    statement  : 'match (n:User) where n.first_name = {first_name} return n',
    parameters : { first_name: "Bob" }
  },
  'match (n:User) where n.first_name = {first_name} return n'
]);
```

### Committing or rolling back

``` js
transaction.commit();
transaction.rollback();
```

Alternatively, a query statement may contain a `commit` or `rollback` property.

``` js
transaction.write({ statement: 'match (n:User) return n', commit: true });

transaction.write({
  statement  : 'match (n:User) where n.first_name = {first_name} return n',
  parameters : { first_name: "Bob" },
  commit     : true
});

```

## Stream per statement

To get a stream per statement, just pass a `callback` function with the statement object.  This works for regular cypher calls and transactions.

``` js
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

cypher([ statement, statement ])
.on('end', function () {
  calls.should.equal(2);
  ended.should.equal(2);
  results.should.equal(4);
  done();
})
.resume();
```

``` js
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
```

## Unsafe Integers

Unsafe integers* are returned as strings.  If your system deals with particularly large or small numbers, this will require special handling.

See "[A note on numbers and the Integer type](https://github.com/neo4j/neo4j-javascript-driver/#a-note-on-numbers-and-the-integer-type)" on the neo4j-javascript-driver README for more information.

\* Unsafe integers are any integers greater than Number.MAX_SAFE_INTEGER or less than Number.MIN_SAFE_INTEGER.
