'use strict';
var should    = require('should');
var normalize = require('../normalize-query-statement');

describe('Query statement normalization', function () {
  var query = 'match (n) return n';

  it('should handle strings', function () {
    var result = normalize(query);
    result.should.eql([{ statement: query }]);
  });

  it('should handle objects', function () {
    var result = normalize({ statement: query });
    result.should.eql([{ statement: query }]);
  });

  it('should handle arrays of strings', function () {
    var result = normalize([ query, query ]);
    result.should.eql([{ statement: query }, { statement: query }]);
  });

  it('should handle arrays of objects', function () {
    var result = normalize([ { statement: query } ]);
    result.should.eql([{ statement: query }]);
  });

  it('should handle commit', function () {
    var result = normalize({ commit: true });
    result.should.eql([{ commit: true }]);
  });

  it('should handle rollback', function () {
    var result = normalize({ rollback: true });
    result.should.eql([{ rollback: true }]);
  });

});
