const { test } = require('node:test')
const events = require('node:events')
const memory = require('./persistence')
const abs = require('./abstract')

abs({
  test,
  persistence: memory
})

// create a memory instance that includes an event emitter
// to test the on-ready functionality
function createAsyncMemory (opts) {
  const mem = memory(opts)
  mem.emitter = new events.EventEmitter()
  mem.on = mem.emitter.on.bind(mem.emitter)
  mem.off = mem.emitter.removeListener.bind(mem.emitter)
  mem.emit = mem.emitter.emit.bind(mem.emitter)
  mem.once = mem.emitter.once.bind(mem.emitter)
  mem.removeAllListeners = mem.emitter.removeAllListeners.bind(mem.emitter)
  // wait 100ms before emitting ready, to simulate setup activities
  setTimeout(() => mem.emit('ready'), 100)
  return mem
}

abs({
  test,
  persistence: createAsyncMemory,
  waitForReady: true
})
