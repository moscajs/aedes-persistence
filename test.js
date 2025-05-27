const { test } = require('node:test')
const memory = require('./persistence')
const AsyncMemory = require('./asyncPersistence')
const abs = require('./abstract')

const persistence = (opts = {}) => {
  opts.broadcastSubscriptions = true
  return memory(opts)
}

// test
abs({
  test,
  persistence: memory
})

// // test wait for ready
abs({
  test,
  persistence,
  waitForReady: true
})

// test asyncPersistence directly without promisified and callbackified
abs({
  test,
  persistence: () => new AsyncMemory(),
  testAsync: true
})
