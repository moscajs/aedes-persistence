const test = require('node:test')
const memory = require('./')
const abs = require('./abstract')

abs({
  test,
  persistence: memory
})
