/**
 * Standardizes a query statement from various formats to an array of objects
 */
module.exports = function normalizeStatement(statement) {
  if(!statement) {
    return statement;
  }
  if (!(statement instanceof Array)) {
    statement = [statement];
  }
  return statement.map(normalizeSingleStatement);
};

function normalizeSingleStatement(statement) {
  // "statement"
  if (typeof statement === 'string') {
    return { statement: statement };
  }
  // { statment: "statement" }
  if(typeof statement == 'object') {
    return statement;
  }
}
