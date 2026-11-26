const { test } = require('node:test')
const AsyncMemory = require('../asyncPersistence.js')
const { getArrayFromStream } = require('../promisified.js')

// The single-level wildcard `+` also matches a zero-length topic level, e.g. `a/+/c` matches `a//c`.

async function withRetained (topics) {
  const instance = new AsyncMemory()
  await instance.setup({ id: 'broker' })
  for (const topic of topics) {
    await instance.storeRetained({ cmd: 'publish', topic, payload: Buffer.from(topic), qos: 0, retain: true })
  }
  return instance
}

async function retainedTopics (instance, patterns) {
  const packets = await getArrayFromStream(instance.createRetainedStreamCombi(patterns))
  return packets.map(p => p.topic).sort()
}

const cases = [
  { pattern: 'a/+/c', topic: 'a//c' },
  { pattern: 'a/+', topic: 'a/' },
  { pattern: '+/b', topic: '/b' },
  { pattern: '+/+/+', topic: '//' }
]

for (const { pattern, topic } of cases) {
  test(`retained: ${pattern} matches ${topic}`, async t => {
    const instance = await withRetained([topic])
    t.assert.deepEqual(await retainedTopics(instance, [pattern]), [topic])
    await instance.destroy()
  })
}

test('retained: + still matches exactly one level', async t => {
  const instance = await withRetained(['a//c', 'a/b/c', 'a/c', 'a/b/b/c'])
  t.assert.deepEqual(await retainedTopics(instance, ['a/+/c']), ['a//c', 'a/b/c'])
  await instance.destroy()
})

test('retained: createRetainedStream matches empty levels', async t => {
  const instance = await withRetained(['a//c'])
  const packets = await getArrayFromStream(instance.createRetainedStream('a/+/c'))
  t.assert.deepEqual(packets.map(p => p.topic), ['a//c'])
  await instance.destroy()
})

test('subscriptionsByTopic: a/+/c matches a//c', async t => {
  const instance = new AsyncMemory()
  await instance.setup({ id: 'broker' })
  await instance.addSubscriptions({ id: 'client' }, [{ topic: 'a/+/c', qos: 1 }])
  const subs = await instance.subscriptionsByTopic('a//c')
  t.assert.deepEqual(subs.map(s => [s.clientId, s.topic]), [['client', 'a/+/c']])
  await instance.destroy()
})