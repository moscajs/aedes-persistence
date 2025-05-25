const { test } = require('node:test')
const memory = require('./persistence')
const abs = require('./abstract')

const persistence = (opts = {}) => {
  opts.broadcastSubscriptions = true
  return memory(opts)
}

abs({
  test,
  persistence: memory
})

abs({
  test,
  persistence,
  waitForReady: true
})
