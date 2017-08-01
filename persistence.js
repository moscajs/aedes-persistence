'use strict'

var util = require('util')
var from2 = require('from2')
var qlobber = require('qlobber')
var Qlobber = qlobber.Qlobber
var QlobberTrue = qlobber.QlobberTrue
var Packet = require('aedes-packet')
var QlobberOpts = {
  wildcard_one: '+',
  wildcard_some: '#',
  separator: '/'
}

function QlobberSub (options) {
  Qlobber.call(this, options)
}

util.inherits(QlobberSub, Qlobber)

QlobberSub.prototype._initial_value = function (val) {
  var topicMap = new Map().set(val.sub.topic, val.sub.qos)
  return new Map().set(val.clientId, topicMap)
}

QlobberSub.prototype._add_value = function (clientMap, val) {
  var topicMap = clientMap.get(val.clientId)
  if (!topicMap) {
    topicMap = new Map()
    clientMap.set(val.clientId, topicMap)
  }
  topicMap.set(val.sub.topic, val.sub.qos)
}

QlobberSub.prototype._add_values = function (destClientMap, originClientMap) {
  originClientMap.forEach(function (originTopicMap, clientId) {
    var destTopicMap = destClientMap.get(clientId)
    if (!destTopicMap) {
      destTopicMap = new Map()
      destClientMap.set(clientId, destTopicMap)
    }
    originTopicMap.forEach(function (qos, topic) {
      destTopicMap.set(topic, qos)
    })
  })
}

QlobberSub.prototype._remove_value = function (clientMap, val) {
  var topicMap = clientMap.get(val.clientId)
  if (topicMap) {
    topicMap.delete(val.topic)
    if (topicMap.size === 0) {
      clientMap.delete(val.clientId)
    }
  }
  return clientMap.size === 0
}

QlobberSub.prototype.test_values = function (clientMap, val) {
  var topicMap = clientMap.get(val.clientId)
  return topicMap && topicMap.has(val.sub.topic)
}

QlobberSub.prototype.match = function (topic) {
  return this._match2(new Map(), topic)
}

function MemoryPersistence () {
  if (!(this instanceof MemoryPersistence)) {
    return new MemoryPersistence()
  }

  this._retained = []
  // clientId -> topic -> qos
  this._subscriptions = new Map()
  this._subscriptionsCount = 0
  this._clientsCount = 0
  this._trie = new QlobberSub(QlobberOpts)
  this._outgoing = {}
  this._incoming = {}
  this._wills = {}
}

function matchTopic (p) {
  return p.topic !== this.topic
}

MemoryPersistence.prototype.storeRetained = function (packet, cb) {
  packet = Object.assign({}, packet)
  this._retained = this._retained.filter(matchTopic, packet)

  if (packet.payload.length > 0) this._retained.push(packet)

  cb(null)
}

function matchingStream (current, pattern) {
  var matcher = new QlobberTrue(QlobberOpts)

  if (Array.isArray(pattern)) {
    pattern.forEach(function (p) {
      matcher.add(p)
    })
  } else {
    matcher.add(pattern)
  }

  return from2.obj(function match (size, next) {
    var entry

    while ((entry = current.shift()) != null) {
      if (matcher.test(entry.topic)) {
        setImmediate(next, null, entry)
        return
      }
    }

    if (!entry) this.push(null)
  })
}

MemoryPersistence.prototype.createRetainedStream = function (pattern) {
  return matchingStream([].concat(this._retained), pattern)
}

MemoryPersistence.prototype.createRetainedStreamCombi = function (patterns) {
  return matchingStream([].concat(this._retained), patterns)
}

MemoryPersistence.prototype.addSubscriptions = function (client, subs, cb) {
  var that = this
  var stored = this._subscriptions.get(client.id)
  var trie = this._trie

  if (!stored) {
    stored = new Map()
    this._subscriptions.set(client.id, stored)
    this._clientsCount++
  }

  subs.forEach(function eachSub (sub) {
    if (sub.qos > 0) {
      var val = { clientId: client.id, sub: sub }
      if (!trie.test(sub.topic, val)) {
        that._subscriptionsCount++
        trie.add(sub.topic, val)
        stored.set(sub.topic, sub.qos)
      }
    } else {
      if (!stored.has(sub.topic)) {
        // TODO: Why aren't we increasing _subscriptionsCount?
        stored.set(sub.topic, sub.qos)
      }
    }
  })

  cb(null, client)
}

MemoryPersistence.prototype.removeSubscriptions = function (client, subs, cb) {
  var that = this
  var stored = this._subscriptions.get(client.id)
  var trie = this._trie

  if (!stored) {
    stored = new Map()
    this._subscriptions.set(client.id, stored)
    // TODO: Do we need to increase _clientsCount?
  }

  subs.forEach(function eachSub (topic) {
    if (stored.delete(topic)) {
      that._subscriptionsCount--
      trie.remove(topic, { clientId: client.id, topic: topic })
    }
  })

  if (stored.size === 0) {
    this._clientsCount--
    this._subscriptions.delete(client.id)
  }

  cb(null, client)
}

MemoryPersistence.prototype.subscriptionsByClient = function (client, cb) {
  var stored = this._subscriptions.get(client.id)
  // TODO: Just returning stored would be more efficient
  var subs = null
  if (stored) {
    subs = []
    stored.forEach(function (qos, topic) {
      subs.push({ topic: topic, qos: qos })
    })
  }
  cb(null, subs, client)
}

MemoryPersistence.prototype.countOffline = function (cb) {
  return cb(null, this._subscriptionsCount, this._clientsCount)
}

MemoryPersistence.prototype.subscriptionsByTopic = function (pattern, cb) {
  var clientMap = this._trie.match(pattern)
  // TODO: Just returning subs would be more efficient
  var subs = []
  clientMap.forEach(function (topicMap, clientId) {
    topicMap.forEach(function (qos, topic) {
      subs.push({ clientId: clientId, topic: topic, qos: qos })
    })
  })
  cb(null, subs)
}

MemoryPersistence.prototype.cleanSubscriptions = function (client, cb) {
  var trie = this._trie
  var stored = this._subscriptions.get(client.id)

  if (stored) {
    stored.forEach(function removeTrie (qos, topic) {
      trie.remove(topic, { clientId: client.id, topic: topic })
    })

    // TODO: What about _subscriptionsCount?

    this._clientsCount--
    this._subscriptions.delete(client.id)
  }

  cb(null, client)
}

MemoryPersistence.prototype.outgoingEnqueue = function (sub, packet, cb) {
  _outgoingEnqueue.call(this, sub, packet)
  process.nextTick(cb)
}

MemoryPersistence.prototype.outgoingEnqueueCombi = function (subs, packet, cb) {
  for (var i = 0; i < subs.length; i++) {
    _outgoingEnqueue.call(this, subs[i], packet)
  }
  process.nextTick(cb)
}

function _outgoingEnqueue (sub, packet) {
  var id = sub.clientId
  var queue = this._outgoing[id] || []

  this._outgoing[id] = queue

  queue[queue.length] = new Packet(packet)
}

MemoryPersistence.prototype.outgoingUpdate = function (client, packet, cb) {
  var i
  var clientId = client.id
  var outgoing = this._outgoing[clientId] || []
  var temp

  this._outgoing[clientId] = outgoing

  for (i = 0; i < outgoing.length; i++) {
    temp = outgoing[i]
    if (temp.brokerId === packet.brokerId &&
      temp.brokerCounter === packet.brokerCounter) {
      temp.messageId = packet.messageId
      return cb(null, client, packet)
    } else if (temp.messageId === packet.messageId) {
      outgoing[i] = packet
      return cb(null, client, packet)
    }
  }

  cb(new Error('no such packet'), client, packet)
}

MemoryPersistence.prototype.outgoingClearMessageId = function (client, packet, cb) {
  var i
  var clientId = client.id
  var outgoing = this._outgoing[clientId] || []
  var temp

  this._outgoing[clientId] = outgoing

  for (i = 0; i < outgoing.length; i++) {
    temp = outgoing[i]
    if (temp.messageId === packet.messageId) {
      outgoing.splice(i, 1)
      return cb(null, temp)
    }
  }

  cb()
}

MemoryPersistence.prototype.outgoingStream = function (client) {
  var queue = [].concat(this._outgoing[client.id] || [])

  return from2.obj(function match (size, next) {
    var entry

    while ((entry = queue.shift()) != null) {
      setImmediate(next, null, entry)
      return
    }

    if (!entry) this.push(null)
  })
}

MemoryPersistence.prototype.incomingStorePacket = function (client, packet, cb) {
  var id = client.id
  var store = this._incoming[id] || {}

  this._incoming[id] = store

  store[packet.messageId] = new Packet(packet)
  store[packet.messageId].messageId = packet.messageId

  cb(null)
}

MemoryPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
  var id = client.id
  var store = this._incoming[id] || {}
  var err = null

  this._incoming[id] = store

  if (!store[packet.messageId]) {
    err = new Error('no such packet')
  }

  cb(err, store[packet.messageId])
}

MemoryPersistence.prototype.incomingDelPacket = function (client, packet, cb) {
  var id = client.id
  var store = this._incoming[id] || {}
  var toDelete = store[packet.messageId]
  var err = null

  if (!toDelete) {
    err = new Error('no such packet')
  } else {
    delete store[packet.messageId]
  }

  cb(err)
}

MemoryPersistence.prototype.putWill = function (client, packet, cb) {
  packet.brokerId = this.broker.id
  packet.clientId = client.id
  this._wills[client.id] = packet
  cb(null, client)
}

MemoryPersistence.prototype.getWill = function (client, cb) {
  cb(null, this._wills[client.id], client)
}

MemoryPersistence.prototype.delWill = function (client, cb) {
  var will = this._wills[client.id]
  delete this._wills[client.id]
  cb(null, will, client)
}

MemoryPersistence.prototype.streamWill = function (brokers) {
  var clients = Object.keys(this._wills)
  var wills = this._wills
  brokers = brokers || {}
  return from2.obj(function match (size, next) {
    var entry

    while ((entry = clients.shift()) != null) {
      if (!brokers[wills[entry].brokerId]) {
        setImmediate(next, null, wills[entry])
        return
      }
    }

    if (!entry) {
      this.push(null)
    }
  })
}

MemoryPersistence.prototype.getClientList = function (topic) {
  var clientSubs = this._subscriptions
  var entries = clientSubs.entries(clientSubs)
  return from2.obj(function match (size, next) {
    var entry
    while (!(entry = entries.next()).done) {
      if (entry.value[1].has(topic)) {
        setImmediate(next, null, entry.value[0])
        return
      }
    }
    next(null, null)
  })
}

MemoryPersistence.prototype.destroy = function (cb) {
  this._retained = null
  if (cb) {
    cb(null)
  }
}

module.exports = MemoryPersistence
