'use strict'

const Packet = require('aedes-packet')
const { once } = require('node:events')
const { PromisifiedPersistence, getArrayFromStream } = require('./promisified.js')

// helper functions

async function doCleanup (t, prInstance) {
  await prInstance.destroy()
  t.diagnostic('instance cleaned up')
}

async function storeRetainedPacket (prInstance, opts = {}) {
  const packet = {
    cmd: 'publish',
    id: prInstance.broker.id,
    topic: opts.topic || 'hello/world',
    payload: opts.payload || Buffer.from('muahah'),
    qos: 0,
    retain: true
  }
  await prInstance.storeRetained(packet)
  return packet
}

async function enqueueAndUpdate (t, prInstance, client, sub, packet, messageId) {
  await prInstance.outgoingEnqueueCombi([sub], packet)
  const updated = new Packet(packet)
  updated.messageId = messageId

  const { reclient, repacket } = await outgoingUpdate(prInstance, client, updated)
  t.assert.equal(reclient, client, 'client matches')
  t.assert.equal(repacket, updated, 'packet matches')
  return repacket
}

function testPacket (t, packet, expected) {
  if (packet.messageId === null) packet.messageId = undefined
  t.assert.equal(packet.messageId, undefined, 'should have an unassigned messageId in queue')
  t.assert.deepEqual(structuredClone(packet), expected, 'must return the packet')
}

function deClassed (obj) {
  return Object.assign({}, obj)
}

async function subscriptionsByClient (prInstance, client) {
  const result = await prInstance.subscriptionsByClient(client)
  if (result.length === 0) {
    return { resubs: null, reClient: client }
  }
  return { resubs: result, reClient: client }
}

async function outgoingUpdate (prInstance, client, updated) {
  const result = await prInstance.outgoingUpdate(client, updated)
  if (result?.reclient) {
    return result
  }
  return { reclient: client, repacket: updated }
}

async function addSubscriptions (prInstance, client, subs) {
  const reClient = await prInstance.addSubscriptions(client, subs)
  if (reClient === undefined) {
    return client
  }
  return reClient
}

async function removeSubscriptions (prInstance, client, subs) {
  const reClient = await prInstance.removeSubscriptions(client, subs)
  if (reClient === undefined) {
    return client
  }
  return reClient
}

async function getWill (prInstance, client) {
  const result = await prInstance.getWill(client)
  return { packet: result, reClient: client }
}

async function putWill (prInstance, client, packet) {
  const result = await prInstance.putWill(client, packet)
  if (result !== undefined) {
    return result
  }
  return client
}

async function delWill (prInstance, client) {
  const result = await prInstance.delWill(client)
  return { packet: result, reClient: client }
}

// start of abstractPersistence
function abstractPersistence (opts) {
  const test = opts.test
  const _persistence = opts.persistence
  const waitForReady = opts.waitForReady

  // requiring it here so it will not error for modules
  // not using the default emitter
  const buildEmitter = opts.buildEmitter || require('mqemitter')
  const testAsync = opts.testAsync

  async function persistence (t) {
    const mq = buildEmitter()
    const broker = {
      id: 'broker-42',
      mq,
      publish: mq.emit.bind(mq),
      subscribe: mq.on.bind(mq),
      unsubscribe: mq.removeListener.bind(mq),
      counter: 0
    }

    const instance = await _persistence()
    if (instance) {
      if (!testAsync) {
      //  prInstance.broker must be set first because setting it triggers
      // the call of instance._setup if aedes-cached-persistence is being used
      // instance._setup then fires the 'ready'event
        instance.broker = broker
        // Wait for ready event, if applicable, to ensure the persistence isn't
        // destroyed while it's still being set up.
        // https://github.com/mcollina/aedes-persistence-redis/issues/41
        if (waitForReady && !instance.ready) {
          await once(instance, 'ready')
        }
        t.diagnostic('instance created')
        return new PromisifiedPersistence(instance)
      }
      // on async persistence the broker is passed to setup
      // no ready event
      await instance.setup(broker)
      t.diagnostic('instance created')
      return instance
    }
    throw new Error('no instance')
  }

  async function matchRetainedWithPattern (t, pattern, opts) {
    const prInstance = await persistence(t)
    const packet = await storeRetainedPacket(prInstance, opts)
    let stream
    if (Array.isArray(pattern)) {
      stream = prInstance.createRetainedStreamCombi(pattern)
    } else {
      stream = prInstance.createRetainedStream(pattern)
    }
    t.diagnostic('created stream')
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [packet], 'must return the packet')
    t.diagnostic('stream was ok')
    await doCleanup(t, prInstance)
  }

  // testing starts here
  test('store and look up retained messages', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, 'hello/world')
  })

  test('look up retained messages with a # pattern', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, '#')
  })

  test('look up retained messages with a hello/world/# pattern', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, 'hello/world/#')
  })

  test('look up retained messages with a + pattern', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, 'hello/+')
  })

  test('look up retained messages with a + as first element', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, '+/world')
  })

  test('look up retained messages with +/#', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, '+/#')
  })

  test('look up retained messages with a + and # pattern with some in between', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, 'hello/+/world/#', { topic: 'hello/there/world/creatures' })
  })

  test('look up retained messages with multiple patterns', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, ['hello/+', 'other/hello'])
  })

  test('look up retained messages with multiple wildcard patterns', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, ['hello/+', 'hel/#', 'hello/world/there/+'])
  })

  test('store multiple retained messages in order', async (t) => {
    t.plan(1000)
    const prInstance = await persistence(t)
    const totalMessages = 1000

    const retained = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: true
    }

    for (let i = 0; i < totalMessages; i++) {
      const packet = new Packet(retained, prInstance.broker)
      await storeRetainedPacket(prInstance, packet)
      t.assert.equal(packet.brokerCounter, i + 1, 'packet stored in order')
    }
    await doCleanup(t, prInstance)
  })

  test('remove retained message', async (t) => {
    t.plan(1)
    const prInstance = await persistence(t)
    await storeRetainedPacket(prInstance, {})
    await storeRetainedPacket(prInstance, {
      payload: Buffer.alloc(0)
    })
    const stream = prInstance.createRetainedStream('#')
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [], 'must return an empty list')
    await doCleanup(t, prInstance)
  })

  test('storing twice a retained message should keep only the last', async (t) => {
    t.plan(1)
    const prInstance = await persistence(t)
    await storeRetainedPacket(prInstance, {})
    const packet = await storeRetainedPacket(prInstance, {
      payload: Buffer.from('ahah')
    })
    const stream = prInstance.createRetainedStream('#')
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [packet], 'must return the last packet')
    await doCleanup(t, prInstance)
  })

  test('Create a new packet while storing a retained message', async (t) => {
    t.plan(1)
    const prInstance = await persistence(t)
    const packet = {
      cmd: 'publish',
      id: prInstance.broker.id,
      topic: opts.topic || 'hello/world',
      payload: opts.payload || Buffer.from('muahah'),
      qos: 0,
      retain: true
    }
    const newPacket = Object.assign({}, packet)

    await prInstance.storeRetained(packet)
    // packet reference change to check if a new packet is stored always
    packet.retain = false
    const stream = prInstance.createRetainedStream('#')
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [newPacket], 'must return the last packet')
    await doCleanup(t, prInstance)
  })

  test('store and look up subscriptions by client', async (t) => {
    t.plan(3)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'noqos',
      qos: 0,
      rh: 0,
      rap: true,
      nl: false
    }]

    const reClient = await addSubscriptions(prInstance, client, subs)
    t.assert.equal(reClient, client, 'client must be the same')
    const { resubs, reClient: reClient2 } = await subscriptionsByClient(prInstance, client)
    t.assert.equal(reClient2, client, 'client must be the same')
    t.assert.deepEqual(resubs, subs)
    await doCleanup(t, prInstance)
  })

  test('remove subscriptions by client', async (t) => {
    t.plan(4)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    const reclient1 = await addSubscriptions(prInstance, client, subs)
    t.assert.equal(reclient1, client, 'client must be the same')
    const reClient2 = await removeSubscriptions(prInstance, client, ['hello'])
    t.assert.equal(reClient2, client, 'client must be the same')
    const { resubs, reClient } = await subscriptionsByClient(prInstance, client)
    t.assert.equal(reClient, client, 'client must be the same')
    t.assert.deepEqual(resubs, [{
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }])
    await doCleanup(t, prInstance)
  })

  test('store and look up subscriptions by topic', async (t) => {
    t.plan(2)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'hello/#',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    const reclient = await addSubscriptions(prInstance, client, subs)
    t.assert.equal(reclient, client, 'client must be the same')
    const resubs = await prInstance.subscriptionsByTopic('hello')
    t.assert.deepEqual(resubs, [{
      clientId: client.id,
      topic: 'hello/#',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      clientId: client.id,
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }])
    await doCleanup(t, prInstance)
  })

  test('get client list after subscriptions', async (t) => {
    t.plan(1)
    const prInstance = await persistence(t)
    const client1 = { id: 'abcde' }
    const client2 = { id: 'efghi' }
    const subs = [{
      topic: 'helloagain',
      qos: 1
    }]

    await addSubscriptions(prInstance, client1, subs)
    await addSubscriptions(prInstance, client2, subs)
    const stream = prInstance.getClientList(subs[0].topic)
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [client1.id, client2.id])
    await doCleanup(t, prInstance)
  })

  test('get client list after an unsubscribe', async (t) => {
    t.plan(1)
    const prInstance = await persistence(t)
    const client1 = { id: 'abcde' }
    const client2 = { id: 'efghi' }
    const subs = [{
      topic: 'helloagain',
      qos: 1
    }]
    await addSubscriptions(prInstance, client1, subs)
    await addSubscriptions(prInstance, client2, subs)
    await removeSubscriptions(prInstance, client2, [subs[0].topic])
    const stream = prInstance.getClientList(subs[0].topic)
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [client1.id])
    await doCleanup(t, prInstance)
  })

  test('get subscriptions list after an unsubscribe', async (t) => {
    t.plan(1)
    const prInstance = await persistence(t)
    const client1 = { id: 'abcde' }
    const client2 = { id: 'efghi' }
    const subs = [{
      topic: 'helloagain',
      qos: 1
    }]
    await addSubscriptions(prInstance, client1, subs)
    await addSubscriptions(prInstance, client2, subs)
    await removeSubscriptions(prInstance, client2, [subs[0].topic])
    const clients = await prInstance.subscriptionsByTopic(subs[0].topic)
    t.assert.deepEqual(clients[0].clientId, client1.id)
    await doCleanup(t, prInstance)
  })

  test('QoS 0 subscriptions, restored but not matched', async (t) => {
    t.plan(2)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 0,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'hello/#',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    await addSubscriptions(prInstance, client, subs)
    const { resubs } = await subscriptionsByClient(prInstance, client)
    t.assert.deepEqual(resubs, subs)
    const resubs2 = await prInstance.subscriptionsByTopic('hello')
    t.assert.deepEqual(resubs2, [{
      clientId: client.id,
      topic: 'hello/#',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }])
    await doCleanup(t, prInstance)
  })

  test('clean subscriptions', async (t) => {
    t.plan(4)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }]

    await addSubscriptions(prInstance, client, subs)
    await prInstance.cleanSubscriptions(client)
    const resubs = await prInstance.subscriptionsByTopic('hello')
    t.assert.deepEqual(resubs, [], 'no subscriptions')
    const { resubs: resubs2 } = await subscriptionsByClient(prInstance, client)
    t.assert.deepEqual(resubs2, null, 'no subscriptions')
    const { subsCount, clientsCount } = await prInstance.countOffline()
    t.assert.equal(subsCount, 0, 'no subscriptions added')
    t.assert.equal(clientsCount, 0, 'no clients added')
    await doCleanup(t, prInstance)
  })

  test('clean subscriptions with no active subscriptions', async (t) => {
    t.plan(4)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }

    await prInstance.cleanSubscriptions(client)
    const resubs = await prInstance.subscriptionsByTopic('hello')
    t.assert.deepEqual(resubs, [], 'no subscriptions')
    const { resubs: resubs2 } = await subscriptionsByClient(prInstance, client)
    t.assert.deepEqual(resubs2, null, 'no subscriptions')
    const { subsCount, clientsCount } = await prInstance.countOffline()
    t.assert.equal(subsCount, 0, 'no subscriptions added')
    t.assert.equal(clientsCount, 0, 'no clients added')
    await doCleanup(t, prInstance)
  })

  test('same topic, different QoS', async (t) => {
    t.plan(5)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 0,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    const reClient = await addSubscriptions(prInstance, client, subs)
    t.assert.equal(reClient, client, 'client must be the same')
    const { resubs } = await subscriptionsByClient(prInstance, client)
    t.assert.deepEqual(resubs, [{
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }])

    const resubs2 = await prInstance.subscriptionsByTopic('hello')
    t.assert.deepEqual(resubs2, [{
      clientId: 'abcde',
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }])

    const { subsCount, clientsCount } = await prInstance.countOffline()
    t.assert.equal(subsCount, 1, 'one subscription added')
    t.assert.equal(clientsCount, 1, 'one client added')
    await doCleanup(t, prInstance)
  })

  test('replace subscriptions', async (t) => {
    t.plan(25)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }
    const topic = 'hello'
    const sub = { topic, rh: 0, rap: true, nl: false }
    const subByTopic = { clientId: client.id, topic, rh: 0, rap: true, nl: false }

    async function check (qos) {
      sub.qos = subByTopic.qos = qos
      const reClient = await addSubscriptions(prInstance, client, [sub])
      t.assert.equal(reClient, client, 'client must be the same')
      const { resubs } = await subscriptionsByClient(prInstance, client)
      t.assert.deepEqual(resubs, [sub])
      const subsForTopic = await prInstance.subscriptionsByTopic(topic)
      t.assert.deepEqual(subsForTopic, qos === 0 ? [] : [subByTopic])
      const { subsCount, clientsCount } = await prInstance.countOffline()
      if (qos === 0) {
        t.assert.equal(subsCount, 0, 'no subscriptions added')
      } else {
        t.assert.equal(subsCount, 1, 'one subscription added')
      }
      t.assert.equal(clientsCount, 1, 'one client added')
    }

    await check(0)
    await check(1)
    await check(2)
    await check(1)
    await check(0)
    await doCleanup(t, prInstance)
  })

  test('replace subscriptions in same call', async (t) => {
    t.plan(5)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }
    const topic = 'hello'
    const subs = [
      { topic, qos: 0, rh: 0, rap: true, nl: false },
      { topic, qos: 1, rh: 0, rap: true, nl: false },
      { topic, qos: 2, rh: 0, rap: true, nl: false },
      { topic, qos: 1, rh: 0, rap: true, nl: false },
      { topic, qos: 0, rh: 0, rap: true, nl: false }
    ]
    const reClient = await addSubscriptions(prInstance, client, subs)
    t.assert.equal(reClient, client, 'client must be the same')
    const { resubs: subsForClient } = await subscriptionsByClient(prInstance, client)
    t.assert.deepEqual(subsForClient, [{ topic, qos: 0, rh: 0, rap: true, nl: false }])
    const subsForTopic = await prInstance.subscriptionsByTopic(topic)
    t.assert.deepEqual(subsForTopic, [])
    const { subsCount, clientsCount } = await prInstance.countOffline()
    t.assert.equal(subsCount, 0, 'no subscriptions added')
    t.assert.equal(clientsCount, 1, 'one client added')
    await doCleanup(t, prInstance)
  })

  test('store and count subscriptions', async (t) => {
    t.plan(11)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }, {
      topic: 'noqos',
      qos: 0
    }]

    const reclient = await addSubscriptions(prInstance, client, subs)
    t.assert.equal(reclient, client, 'client must be the same')
    const { subsCount, clientsCount } = await prInstance.countOffline()
    t.assert.equal(subsCount, 2, 'two subscriptions added')
    t.assert.equal(clientsCount, 1, 'one client added')
    await removeSubscriptions(prInstance, client, ['hello'])
    const { subsCount: subsCount2, clientsCount: clientsCount2 } = await prInstance.countOffline()
    t.assert.equal(subsCount2, 1, 'one subscription added')
    t.assert.equal(clientsCount2, 1, 'one client added')
    await removeSubscriptions(prInstance, client, ['matteo'])
    const { subsCount: subsCount3, clientsCount: clientsCount3 } = await prInstance.countOffline()
    t.assert.equal(subsCount3, 0, 'zero subscriptions added')
    t.assert.equal(clientsCount3, 1, 'one client added')
    await removeSubscriptions(prInstance, client, ['noqos'])
    const { subsCount: subsCount4, clientsCount: clientsCount4 } = await prInstance.countOffline()
    t.assert.equal(subsCount4, 0, 'zero subscriptions added')
    t.assert.equal(clientsCount4, 0, 'zero clients added')
    await removeSubscriptions(prInstance, client, ['noqos'])
    const { subsCount: subsCount5, clientsCount: clientsCount5 } = await prInstance.countOffline()
    t.assert.equal(subsCount5, 0, 'zero subscriptions added')
    t.assert.equal(clientsCount5, 0, 'zero clients added')
    await doCleanup(t, prInstance)
  })

  test('count subscriptions with two clients', async (t) => {
    t.plan(26)
    const prInstance = await persistence(t)
    const client1 = { id: 'abcde' }
    const client2 = { id: 'fghij' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }, {
      topic: 'noqos',
      qos: 0
    }]

    async function remove (client, subs, expectedSubs, expectedClients) {
      const reClient = await removeSubscriptions(prInstance, client, subs)
      t.assert.equal(reClient, client, 'client must be the same')
      const { subsCount, clientsCount } = await prInstance.countOffline()
      t.assert.equal(subsCount, expectedSubs, 'subscriptions added')
      t.assert.equal(clientsCount, expectedClients, 'clients added')
    }

    const reClient1 = await addSubscriptions(prInstance, client1, subs)
    t.assert.equal(reClient1, client1, 'client must be the same')
    const reClient2 = await addSubscriptions(prInstance, client2, subs)
    t.assert.equal(reClient2, client2, 'client must be the same')
    await remove(client1, ['foobar'], 4, 2)
    await remove(client1, ['hello'], 3, 2)
    await remove(client1, ['hello'], 3, 2)
    await remove(client1, ['matteo'], 2, 2)
    await remove(client1, ['noqos'], 2, 1)
    await remove(client2, ['hello'], 1, 1)
    await remove(client2, ['matteo'], 0, 1)
    await remove(client2, ['noqos'], 0, 0)
    await doCleanup(t, prInstance)
  })

  test('add duplicate subs to persistence for qos > 0', async (t) => {
    t.plan(3)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }
    const topic = 'hello'
    const subs = [{
      topic,
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    const reClient = await addSubscriptions(prInstance, client, subs)
    t.assert.equal(reClient, client, 'client must be the same')
    const reClient2 = await addSubscriptions(prInstance, client, subs)
    t.assert.equal(reClient2, client, 'client must be the same')
    subs[0].clientId = client.id
    const subsForTopic = await prInstance.subscriptionsByTopic(topic)
    t.assert.deepEqual(subsForTopic, subs)
    await doCleanup(t, prInstance)
  })

  test('add duplicate subs to persistence for qos 0', async (t) => {
    t.plan(3)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }
    const topic = 'hello'
    const subs = [{
      topic,
      qos: 0,
      rh: 0,
      rap: true,
      nl: false
    }]

    const reClient = await addSubscriptions(prInstance, client, subs)
    t.assert.equal(reClient, client, 'client must be the same')
    const reClient2 = await addSubscriptions(prInstance, client, subs)
    t.assert.equal(reClient2, client, 'client must be the same')
    const { resubs: subsForClient } = await subscriptionsByClient(prInstance, client)
    t.assert.deepEqual(subsForClient, subs)
    await doCleanup(t, prInstance)
  })

  test('get topic list after concurrent subscriptions of a client', async (t) => {
    t.plan(1)
    const prInstance = await persistence(t)
    const client = { id: 'abcde' }
    const subs1 = [{
      topic: 'hello1',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]
    const subs2 = [{
      topic: 'hello2',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]
    await addSubscriptions(prInstance, client, subs1)
    await addSubscriptions(prInstance, client, subs2)
    const { resubs } = await subscriptionsByClient(prInstance, client)
    resubs.sort((a, b) => a.topic.localeCompare(b.topic, 'en'))
    t.assert.deepEqual(resubs, [subs1[0], subs2[0]])
    await doCleanup(t, prInstance)
  })

  test('add outgoing packet and stream it', async (t) => {
    t.plan(2)
    const prInstance = await persistence(t)
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 42
    }
    const expected = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: false,
      dup: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 42,
      messageId: undefined
    }

    await prInstance.outgoingEnqueue(sub, packet)
    const stream = prInstance.outgoingStream(client)
    const list = await getArrayFromStream(stream)
    testPacket(t, list[0], expected)
    await doCleanup(t, prInstance)
  })

  test('add outgoing packet for multiple subs and stream to all', async (t) => {
    t.plan(4)
    const prInstance = await persistence(t)
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const sub2 = {
      clientId: 'fghih',
      topic: 'hello',
      qos: 1
    }
    const subs = [sub, sub2]
    const client = {
      id: sub.clientId
    }
    const client2 = {
      id: sub2.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 42
    }
    const expected = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: false,
      dup: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 42,
      messageId: undefined
    }

    await prInstance.outgoingEnqueueCombi(subs, packet)
    const stream = prInstance.outgoingStream(client)
    const list = await getArrayFromStream(stream)
    testPacket(t, list[0], expected)

    const stream2 = prInstance.outgoingStream(client2)
    const list2 = await getArrayFromStream(stream2)
    testPacket(t, list2[0], expected)
    await doCleanup(t, prInstance)
  })

  test('add outgoing packet as a string and pump', async (t) => {
    t.plan(7)
    const prInstance = await persistence(t)
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet1 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 10
    }
    const packet2 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('matteo'),
      qos: 1,
      retain: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 50
    }
    const queue = []

    const updated1 = await enqueueAndUpdate(t, prInstance, client, sub, packet1, 42)
    const updated2 = await enqueueAndUpdate(t, prInstance, client, sub, packet2, 43)
    const stream = prInstance.outgoingStream(client)

    async function clearQueue (data) {
      const { repacket } = await outgoingUpdate(prInstance, client, data)
      t.diagnostic('packet received')
      queue.push(repacket)
    }

    const list = await getArrayFromStream(stream)
    for (const data of list) {
      await clearQueue(data)
    }
    t.assert.equal(queue.length, 2)
    t.assert.deepEqual(deClassed(queue[0]), deClassed(updated1))
    t.assert.deepEqual(deClassed(queue[1]), deClassed(updated2))
    await doCleanup(t, prInstance)
  })

  test('add outgoing packet as a string and stream', async (t) => {
    t.plan(2)
    const prInstance = await persistence(t)
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: 'world',
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 42
    }
    const expected = {
      cmd: 'publish',
      topic: 'hello',
      payload: 'world',
      qos: 1,
      retain: false,
      dup: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 42,
      messageId: undefined
    }

    await prInstance.outgoingEnqueueCombi([sub], packet)
    const stream = prInstance.outgoingStream(client)
    const list = await getArrayFromStream(stream)
    testPacket(t, list[0], expected)
    await doCleanup(t, prInstance)
  })

  test('add outgoing packet and stream it twice', async (t) => {
    t.plan(5)
    const prInstance = await persistence(t)
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 42,
      messageId: 4242
    }
    const expected = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: false,
      dup: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 42,
      messageId: undefined
    }

    await prInstance.outgoingEnqueueCombi([sub], packet)
    const stream = prInstance.outgoingStream(client)
    const list = await getArrayFromStream(stream)
    testPacket(t, list[0], expected)
    const stream2 = prInstance.outgoingStream(client)
    const list2 = await getArrayFromStream(stream2)
    testPacket(t, list2[0], expected)
    t.assert.notEqual(packet, expected, 'packet must be a different object')
    await doCleanup(t, prInstance)
  })

  test('add outgoing packet and update messageId', async (t) => {
    t.plan(5)
    const prInstance = await persistence(t)
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 42
    }

    const updated = await enqueueAndUpdate(t, prInstance, client, sub, packet, 42)
    updated.messageId = undefined
    const stream = prInstance.outgoingStream(client)
    const list = await getArrayFromStream(stream)
    list[0].messageId = undefined
    t.assert.notEqual(list[0], updated, 'must not be the same object')
    t.assert.deepEqual(deClassed(list[0]), deClassed(updated), 'must return the packet')
    t.assert.equal(list.length, 1, 'must return only one packet')
    await doCleanup(t, prInstance)
  })

  test('add 2 outgoing packet and clear messageId', async (t) => {
    t.plan(10)
    const prInstance = await persistence(t)
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet1 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 42
    }
    const packet2 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('matteo'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 43
    }

    const updated1 = await enqueueAndUpdate(t, prInstance, client, sub, packet1, 42)
    const updated2 = await enqueueAndUpdate(t, prInstance, client, sub, packet2, 43)
    const pkt = await prInstance.outgoingClearMessageId(client, updated1)
    t.assert.deepEqual(pkt.messageId, 42, 'must have the same messageId')
    t.assert.deepEqual(pkt.payload.toString(), packet1.payload.toString(), 'must have original payload')
    t.assert.deepEqual(pkt.topic, packet1.topic, 'must have original topic')
    const stream = prInstance.outgoingStream(client)
    updated2.messageId = undefined
    const list = await getArrayFromStream(stream)
    list[0].messageId = undefined
    t.assert.notEqual(list[0], updated2, 'must not be the same object')
    t.assert.deepEqual(deClassed(list[0]), deClassed(updated2), 'must return the packet')
    t.assert.equal(list.length, 1, 'must return only one packet')
    await doCleanup(t, prInstance)
  })

  test('add many outgoing packets and clear messageIds', async (t) => {
    // t.plan() is called below after we know the high watermark
    const prInstance = await persistence(t)
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false
    }

    // we just need a stream to figure out the high watermark
    const stream = prInstance.outgoingStream(client)
    const total = stream.readableHighWaterMark * 2
    t.plan(total * 2)

    for (let i = 0; i < total; i++) {
      const p = new Packet(packet, prInstance.broker)
      p.messageId = i
      await prInstance.outgoingEnqueue(sub, p)
      await outgoingUpdate(prInstance, client, p)
    }

    const queued = await getArrayFromStream(prInstance.outgoingStream(client))
    t.assert.equal(queued.length, total, `outgoing queue must hold ${total} items`)

    for await (const p of (prInstance.outgoingStream(client))) {
      const received = await prInstance.outgoingClearMessageId(client, p)
      t.assert.deepEqual(received, p, 'must return the packet')
    }

    const queued2 = await getArrayFromStream(prInstance.outgoingStream(client))
    t.assert.equal(queued2.length, 0, 'outgoing queue is empty')
    await doCleanup(t, prInstance)
  })

  test('update to publish w/ same messageId', async (t) => {
    t.plan(5)
    const prInstance = await persistence(t)
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet1 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 2,
      dup: false,
      length: 14,
      retain: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 42,
      messageId: 42
    }
    const packet2 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 2,
      dup: false,
      length: 14,
      retain: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 50,
      messageId: 42
    }

    await prInstance.outgoingEnqueue(sub, packet1)
    await prInstance.outgoingEnqueue(sub, packet2)
    await outgoingUpdate(prInstance, client, packet1)
    await outgoingUpdate(prInstance, client, packet2)
    const stream = prInstance.outgoingStream(client)
    const list = await getArrayFromStream(stream)
    t.assert.equal(list.length, 2, 'must have two items in queue')
    t.assert.equal(list[0].brokerCounter, packet1.brokerCounter, 'brokerCounter must match')
    t.assert.equal(list[0].messageId, packet1.messageId, 'messageId must match')
    t.assert.equal(list[1].brokerCounter, packet2.brokerCounter, 'brokerCounter must match')
    t.assert.equal(list[1].messageId, packet2.messageId, 'messageId must match')
    await doCleanup(t, prInstance)
  })

  test('update to pubrel', async (t) => {
    t.plan(3)
    const prInstance = await persistence(t)
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 2,
      dup: false,
      length: 14,
      retain: false,
      brokerId: prInstance.broker.id,
      brokerCounter: 42
    }

    await prInstance.outgoingEnqueueCombi([sub], packet)
    const updated = new Packet(packet)
    updated.messageId = 42
    const { reclient, repacket } = await outgoingUpdate(prInstance, client, updated)
    t.assert.equal(reclient, client, 'client matches')
    t.assert.equal(repacket, updated, 'packet matches')

    const pubrel = {
      cmd: 'pubrel',
      messageId: updated.messageId
    }

    await outgoingUpdate(prInstance, client, pubrel)
    const stream = prInstance.outgoingStream(client)
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [pubrel], 'must return the packet')
    await doCleanup(t, prInstance)
  })

  test('add incoming packet, get it, and clear with messageId', async (t) => {
    t.plan(3)
    const prInstance = await persistence(t)
    const client = {
      id: 'abcde'
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 2,
      dup: false,
      length: 14,
      retain: false,
      messageId: 42
    }
    await prInstance.incomingStorePacket(client, packet)
    const retrieved = await prInstance.incomingGetPacket(client, {
      messageId: packet.messageId
    })
    // adjusting the objects so they match
    delete retrieved.brokerCounter
    delete retrieved.brokerId
    delete packet.length
    // strip the class identifier from the packet
    const result = structuredClone(retrieved)
    // Convert Uint8 to Buffer for comparison
    result.payload = Buffer.from(result.payload)
    t.assert.deepEqual(result, packet, 'retrieved packet must be deeply equal')
    t.assert.notEqual(retrieved, packet, 'retrieved packet must not be the same object')
    await prInstance.incomingDelPacket(client, retrieved)

    try {
      await prInstance.incomingGetPacket(client, {
        messageId: packet.messageId
      })
      t.assert.ok(false, 'must error')
    } catch (err) {
      t.assert.ok(err, 'must error')
      await doCleanup(t, prInstance)
    }
  })

  test('store, fetch and delete will message', async (t) => {
    t.plan(7)
    const prInstance = await persistence(t)
    const client = {
      id: '12345'
    }
    const expected = {
      topic: 'hello/died',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    const c = await putWill(prInstance, client, expected)
    t.assert.equal(c, client, 'client matches')
    const { packet: p1, reClient: c1 } = await getWill(prInstance, client)
    t.assert.deepEqual(p1, expected, 'will matches')
    t.assert.equal(c1, client, 'client matches')
    client.brokerId = p1.brokerId
    const { packet: p2, reClient: c2 } = await delWill(prInstance, client)
    t.assert.deepEqual(p2, expected, 'will matches')
    t.assert.equal(c2, client, 'client matches')
    const { packet: p3, reClient: c3 } = await getWill(prInstance, client)
    t.assert.ok(!p3, 'no will after del')
    t.assert.equal(c3, client, 'client matches')
    await doCleanup(t, prInstance)
  })

  test('stream all will messages', async (t) => {
    t.plan(3)
    const prInstance = await persistence(t)
    const client = {
      id: '12345',
      brokerId: prInstance.broker.id
    }
    const toWrite = {
      topic: 'hello/died',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    const expected = {
      clientId: client.id,
      brokerId: prInstance.broker.id,
      topic: 'hello/died',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    const c = await putWill(prInstance, client, toWrite)
    t.assert.equal(c, client, 'client matches')
    const stream = prInstance.streamWill()
    const list = await getArrayFromStream(stream)
    t.assert.equal(list.length, 1, 'must return only one packet')
    t.assert.deepEqual(list[0], expected, 'packet matches')
    await delWill(prInstance, client)
    await doCleanup(t, prInstance)
  })

  test('stream all will message for unknown brokers', async (t) => {
    t.plan(4)
    const prInstance = await persistence(t)
    const originalId = prInstance.broker.id
    const client = {
      id: '42',
      brokerId: prInstance.broker.id
    }
    const anotherClient = {
      id: '24',
      brokerId: prInstance.broker.id
    }
    const toWrite1 = {
      topic: 'hello/died42',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }
    const toWrite2 = {
      topic: 'hello/died24',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }
    const expected = {
      clientId: client.id,
      brokerId: originalId,
      topic: 'hello/died42',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    const c = await putWill(prInstance, client, toWrite1)
    t.assert.equal(c, client, 'client matches')
    prInstance.broker.id = 'anotherBroker'
    const c2 = await putWill(prInstance, anotherClient, toWrite2)
    t.assert.equal(c2, anotherClient, 'client matches')
    const stream = prInstance.streamWill({
      anotherBroker: Date.now()
    })
    const list = await getArrayFromStream(stream)
    t.assert.equal(list.length, 1, 'must return only one packet')
    t.assert.deepEqual(list[0], expected, 'packet matches')
    await delWill(prInstance, client)
    await doCleanup(t, prInstance)
  })

  test('delete wills from dead brokers', async (t) => {
    const prInstance = await persistence(t)
    const client = {
      id: '42'
    }

    const toWrite1 = {
      topic: 'hello/died42',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    const c = await putWill(prInstance, client, toWrite1)
    t.assert.equal(c, client, 'client matches')
    prInstance.broker.id = 'anotherBroker'
    client.brokerId = prInstance.broker.id
    await delWill(prInstance, client)
    await doCleanup(t, prInstance)
  })

  test('do not error if unkown messageId in outoingClearMessageId', async (t) => {
    const prInstance = await persistence(t)
    const client = {
      id: 'abc-123'
    }

    await prInstance.outgoingClearMessageId(client, 42)
    await doCleanup(t, prInstance)
  })
}

module.exports = abstractPersistence
