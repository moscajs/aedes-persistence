'use strict'

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

function MemoryPersistence () {
  if (!(this instanceof MemoryPersistence)) {
    return new MemoryPersistence()
  }

  this._retained = []
  this._subscriptions = []
  this._subscriptionsCount = 0
  this._clientsCount = 0
  this._trie = new Qlobber(QlobberOpts)
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
    pattern.map(function (p) {
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

function checkIfSubAdded (sub, addedSubs) {
  for (var i = 0; i < addedSubs.length; i++) {
    if (sub.topic === addedSubs[i].topic && sub.clientId === addedSubs[i].clientId) {
      return true
    }
  }
  return false
}

MemoryPersistence.prototype.addSubscriptions = function (client, subs, cb) {
  var that = this
  var stored = this._subscriptions[client.id]
  var trie = this._trie

  if (!stored) {
    stored = []
    this._subscriptions[client.id] = stored
    this._clientsCount++
  }

  subs.map(function mapSub (sub) {
    return {
      clientId: client.id,
      topic: sub.topic,
      qos: sub.qos
    }
  }).forEach(function eachSub (sub) {
    if (sub.qos > 0) {
      if (!checkIfSubAdded(sub, trie.match(sub.topic))) {
        that._subscriptionsCount++
        trie.add(sub.topic, sub)
        stored.push(sub)
      }
    } else {
      if (!checkIfSubAdded(sub, that._subscriptions[client.id])) {
        stored.push(sub)
      }
    }
  })

  cb(null, client)
}

MemoryPersistence.prototype.removeSubscriptions = function (client, subs, cb) {
  var that = this
  var stored = this._subscriptions[client.id]
  var trie = this._trie

  if (!stored) {
    stored = []
    this._subscriptions[client.id] = stored
  }

  this._subscriptions[client.id] = stored.filter(function noSub (storedSub) {
    var toKeep = subs.indexOf(storedSub.topic) < 0
    if (!toKeep) {
      that._subscriptionsCount--
      trie.remove(storedSub.topic, storedSub)
    }
    return toKeep
  })

  if (this._subscriptions[client.id].length === 0) {
    this._clientsCount--
    delete this._subscriptions[client.id]
  }

  cb(null, client)
}

function toSubObj (sub) {
  return {
    topic: sub.topic,
    qos: sub.qos
  }
}

MemoryPersistence.prototype.subscriptionsByClient = function (client, cb) {
  var subs = this._subscriptions[client.id] || null
  if (subs) {
    subs = subs.map(toSubObj)
  }
  cb(null, subs, client)
}

MemoryPersistence.prototype.countOffline = function (cb) {
  return cb(null, this._subscriptionsCount, this._clientsCount)
}

MemoryPersistence.prototype.subscriptionsByTopic = function (pattern, cb) {
  cb(null, this._trie.match(pattern))
}

MemoryPersistence.prototype.cleanSubscriptions = function (client, cb) {
  var trie = this._trie

  if (this._subscriptions[client.id]) {
    this._subscriptions[client.id].forEach(function removeTrie (sub) {
      trie.remove(sub.topic, sub)
    })

    this._clientsCount--
    delete this._subscriptions[client.id]
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
  var keys = Object.keys(clientSubs)
  return from2.obj(function match (size, next) {
    var clientKey
    while ((clientKey = keys.shift()) != null) {
      var subs = clientSubs[clientKey]
      var current = 0
      while (current < subs.length) {
        if (subs[current].topic === topic) {
          setImmediate(next, null, subs[current].clientId)
          current++
          return
        }
      }
    }
    if (!clientKey) {
      next(null, null)
    }
  })
}

MemoryPersistence.prototype.destroy = function (cb) {
  this._retained = null
  if (cb) {
    cb(null)
  }
}

module.exports = MemoryPersistence
