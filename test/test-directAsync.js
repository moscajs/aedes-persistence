const { test } = require('node:test')
const AsyncMemory = require('../asyncPersistence.js')
const abs = require('../abstract.js')

// test asyncPersistence directly without promisified and callbackPersistence
abs({
  test,
  persistence: () => new AsyncMemory(),
  testAsync: true
})
