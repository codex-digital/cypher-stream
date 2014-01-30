# cypher-stream

Streams cypher query results as easy to manage objects.

## Basic usage

``` js
var neo4j_cypher_stream = require('cypher-stream');
var cypher = neo4j_cypher_stream('http://localhost:7474/db/data/cypher');

cypher('match (user:User) return user')
  .on('data', function (result){
    console.log(result.user.first_name);
  })
  .on('end', function() {
    console.log('all done');
  })
;
```
