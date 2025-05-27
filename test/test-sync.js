const { test } = require('node:test')
const memory = require('../persistence.js')
const abs = require('../abstract.js')

// callbacks no waiting for ready
abs({
  test,
  persistence: memory
})
