const { Readable } = require('stream')
const QlobberSub = require('qlobber/aedes/qlobber-sub')
const { QlobberTrue } = require('qlobber')
const Packet = require('aedes-packet')
const QlobberOpts = {
  wildcard_one: '+',
  wildcard_some: '#',
  separator: '/'
}

function * multiIterables (iterables) {
  for (const iter of iterables) {
    yield * iter
  }
}

function * retainedMessagesByPattern (retained, pattern) {
  const qlobber = new QlobberTrue(QlobberOpts)
  qlobber.add(pattern)

  for (const packet of retained) {
    if (qlobber.test(packet.topic)) {
      yield packet
    }
  }
}

function * willsByBrokers (wills, brokers) {
  for (const will of wills.values()) {
    if (!brokers[will.brokerId]) {
      yield will
    }
  }
}

function * clientListbyTopic (subscriptions, topic) {
  for (const [clientId, topicMap] of subscriptions) {
    if (topicMap.has(topic)) {
      yield clientId
    }
  }
}

class MemoryPersistence {
  constructor () {
    // [ retainedPacket ]
    this._retained = []
    // Map( clientId -> Map( topic -> qos ))
    this._subscriptions = new Map()
    // { clientId  > [ packet ] }
    this._outgoing = {}
    // { clientId -> { packetId -> Packet } }
    this._incoming = {}
    // Map( clientId -> will )
    this._wills = new Map()
    this._clientsCount = 0
    this._trie = new QlobberSub(QlobberOpts)
  }

  storeRetained (pkt, cb) {
    const packet = Object.assign({}, pkt)
    this._retained = this._retained.filter(matchTopic, packet)

    if (packet.payload.length > 0) { this._retained.push(packet) }

    cb(null)
  }

  createRetainedStreamCombi (patterns) {
    const iterables = patterns.map((p) => {
      return retainedMessagesByPattern(this._retained, p)
    })
    return Readable.from(multiIterables(iterables))
  }

  createRetainedStream (pattern) {
    return Readable.from(retainedMessagesByPattern(this._retained, pattern))
  }

  addSubscriptions (client, subs, cb) {
    let stored = this._subscriptions.get(client.id)
    const trie = this._trie

    if (!stored) {
      stored = new Map()
      this._subscriptions.set(client.id, stored)
      this._clientsCount++
    }

    for (const sub of subs) {
      const qos = stored.get(sub.topic)
      const hasQoSGreaterThanZero = (qos !== undefined) && (qos > 0)
      if (sub.qos > 0) {
        trie.add(sub.topic, {
          clientId: client.id,
          topic: sub.topic,
          qos: sub.qos
        })
      } else if (hasQoSGreaterThanZero) {
        trie.remove(sub.topic, {
          clientId: client.id,
          topic: sub.topic
        })
      }
      stored.set(sub.topic, sub.qos)
    }

    cb(null, client)
  }

  removeSubscriptions (client, subs, cb) {
    const stored = this._subscriptions.get(client.id)
    const trie = this._trie

    if (stored) {
      for (const topic of subs) {
        const qos = stored.get(topic)
        if (qos !== undefined) {
          if (qos > 0) {
            trie.remove(topic, { clientId: client.id, topic })
          }
          stored.delete(topic)
        }
      }

      if (stored.size === 0) {
        this._clientsCount--
        this._subscriptions.delete(client.id)
      }
    }

    cb(null, client)
  }

  subscriptionsByClient (client, cb) {
    let subs = null
    const stored = this._subscriptions.get(client.id)
    if (stored) {
      subs = []
      for (const topicAndQos of stored) {
        subs.push({ topic: topicAndQos[0], qos: topicAndQos[1] })
      }
    }
    cb(null, subs, client)
  }

  countOffline (cb) {
    return cb(null, this._trie.subscriptionsCount, this._clientsCount)
  }

  subscriptionsByTopic (pattern, cb) {
    cb(null, this._trie.match(pattern))
  }

  cleanSubscriptions (client, cb) {
    const trie = this._trie
    const stored = this._subscriptions.get(client.id)

    if (stored) {
      for (const topicAndQos of stored) {
        if (topicAndQos[1] > 0) {
          const topic = topicAndQos[0]
          trie.remove(topic, { clientId: client.id, topic })
        }
      }

      this._clientsCount--
      this._subscriptions.delete(client.id)
    }

    cb(null, client)
  }

  outgoingEnqueue (sub, packet, cb) {
    _outgoingEnqueue.call(this, sub, packet)
    process.nextTick(cb)
  }

  outgoingEnqueueCombi (subs, packet, cb) {
    for (let i = 0; i < subs.length; i++) {
      _outgoingEnqueue.call(this, subs[i], packet)
    }
    process.nextTick(cb)
  }

  outgoingUpdate (client, packet, cb) {
    const clientId = client.id
    const outgoing = this._outgoing[clientId] || []
    let temp

    this._outgoing[clientId] = outgoing

    for (let i = 0; i < outgoing.length; i++) {
      temp = outgoing[i]
      if (temp.brokerId === packet.brokerId) {
        if (temp.brokerCounter === packet.brokerCounter) {
          temp.messageId = packet.messageId
          return cb(null, client, packet)
        }
        /*
                Maximum of messageId (packet identifier) is 65535 and will be rotated,
                brokerCounter is to ensure the packet identifier be unique.
                The for loop is going to search which packet messageId should be updated
                in the _outgoing queue.
                If there is a case that brokerCounter is different but messageId is same,
                we need to let the loop keep searching
                */
      } else if (temp.messageId === packet.messageId) {
        outgoing[i] = packet
        return cb(null, client, packet)
      }
    }

    cb(new Error('no such packet'), client, packet)
  }

  outgoingClearMessageId (client, packet, cb) {
    const clientId = client.id
    const outgoing = this._outgoing[clientId] || []
    let temp

    this._outgoing[clientId] = outgoing

    for (let i = 0; i < outgoing.length; i++) {
      temp = outgoing[i]
      if (temp.messageId === packet.messageId) {
        outgoing.splice(i, 1)
        return cb(null, temp)
      }
    }

    cb()
  }

  outgoingStream (client) {
    return Readable.from(this._outgoing[client.id] || [])
  }

  incomingStorePacket (client, packet, cb) {
    const id = client.id
    const store = this._incoming[id] || {}

    this._incoming[id] = store

    store[packet.messageId] = new Packet(packet)
    store[packet.messageId].messageId = packet.messageId

    cb(null)
  }

  incomingGetPacket (client, packet, cb) {
    const id = client.id
    const store = this._incoming[id] || {}
    let err = null

    this._incoming[id] = store

    if (!store[packet.messageId]) {
      err = new Error('no such packet')
    }

    cb(err, store[packet.messageId])
  }

  incomingDelPacket (client, packet, cb) {
    const id = client.id
    const store = this._incoming[id] || {}
    const toDelete = store[packet.messageId]
    let err = null

    if (!toDelete) {
      err = new Error('no such packet')
    } else {
      delete store[packet.messageId]
    }

    cb(err)
  }

  putWill (client, packet, cb) {
    packet.brokerId = this.broker.id
    packet.clientId = client.id
    this._wills.set(client.id, packet)
    cb(null, client)
  }

  getWill (client, cb) {
    cb(null, this._wills.get(client.id), client)
  }

  delWill (client, cb) {
    const will = this._wills.get(client.id)
    this._wills.delete(client.id)
    cb(null, will, client)
  }

  streamWill (brokers = {}) {
    return Readable.from(willsByBrokers(this._wills, brokers))
  }

  getClientList (topic) {
    return Readable.from(clientListbyTopic(this._subscriptions, topic))
  }

  destroy (cb) {
    this._retained = null
    if (cb) {
      cb(null)
    }
  }
}

function matchTopic (p) {
  return p.topic !== this.topic
}

function _outgoingEnqueue (sub, packet) {
  const id = sub.clientId
  const queue = this._outgoing[id] || []

  this._outgoing[id] = queue
  const p = new Packet(packet)
  queue[queue.length] = p
}

module.exports = () => { return new MemoryPersistence() }
module.exports.Packet = Packet
