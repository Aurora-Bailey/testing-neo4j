const neo4j = require('neo4j-driver').v1
const credentials = require('./credentials.json')

// Close the driver when application exits.
// This closes all used network connections.
// driver.close()

class Neo4jConnect {
  constructor () {
    this._driver = false
  }

  close () {
    if (this._driver) this._driver.close()
  }

  driver () {
    if (this._driver) return this._driver
    else {
      this._driver = neo4j.driver(credentials.host, neo4j.auth.basic(credentials.user, credentials.password))
      return this._driver
    }
  }
}

module.exports = new Neo4jConnect()
