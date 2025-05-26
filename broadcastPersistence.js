'use strict'

const Packet = require('aedes-packet')
// System topics for subscription management
const TOPIC_ADD_SUBSCRIPTION = '$SYS/sub/add'
const TOPIC_REMOVE_SUBSCRIPTION = '$SYS/sub/rm'
const TOPIC_SUBSCRIPTION_PATTERN = '$SYS/sub/+'
// Constants to increase readability
const SUBSCRIBE = true
const UNSUBSCRIBE = false

function getKey (clientId, isSub, topic) {
  return `${clientId}-${isSub ? 'sub_' : 'unsub_'}${topic || ''}`
}

function brokerPublish (broker, topic, clientId, subs, cb) {
  const encoded = JSON.stringify({ clientId, subs })
  const packet = new Packet({
    topic,
    payload: encoded
  })
  broker.publish(packet, cb)
}

class BroadcastPersistence {
  // private members start with #
  #waiting
  #broker
  #trie
  #onSubMessage

  constructor (broker, trie) {
    this.#waiting = new Map()
    this.#broker = broker
    this.#trie = trie
    this.#onSubMessage = this.#onMessage.bind(this)
  }

  #onMessage (packet, cb) {
    const decoded = JSON.parse(packet.payload)
    const clientId = decoded.clientId
    const isAddSubscription = packet.topic === TOPIC_ADD_SUBSCRIPTION

    for (let i = 0; i < decoded.subs.length; i++) {
      const sub = decoded.subs[i]
      sub.clientId = clientId

      if (isAddSubscription) {
        if (sub.qos > 0) {
          this.#trie.add(sub.topic, sub)
        } else {
          this.#trie.remove(sub.topic, sub)
        }
      } else if (packet.topic === TOPIC_REMOVE_SUBSCRIPTION) {
        this.#trie.remove(sub.topic, sub)
      }
    }

    if (decoded.subs.length > 0) {
      const key = getKey(clientId, isAddSubscription, decoded.subs[0].topic)
      const waiting = this.#waiting.get(key)
      if (waiting) {
        this.#waiting.delete(key)
        process.nextTick(waiting)
      }
    }
    cb()
  }

  #waitFor (client, isSub, topic, cb) {
    this.#waiting.set(getKey(client.id, isSub, topic), cb)
  }

  addedSubscriptions (client, subs, cb) {
    if (subs.length === 0) {
      return cb(null, client)
    }

    let errored = false

    this.#waitFor(client, SUBSCRIBE, subs[0].topic, (err) => {
      if (!errored && err) {
        return cb(err)
      }
      if (!errored) {
        cb(null, client)
      }
    })

    brokerPublish(this.#broker, TOPIC_ADD_SUBSCRIPTION, client.id, subs, (err) => {
      if (err) {
        errored = true
        cb(err)
      }
    })
  }

  removedSubscriptions (client, subs, cb) {
    let errored = false
    let key = subs

    if (subs.length > 0) {
      key = subs[0]
    }

    this.#waitFor(client, UNSUBSCRIBE, key, (err) => {
      if (!errored && err) {
        return cb(err)
      }
      if (!errored) {
        cb(null, client)
      }
    })

    const mappedSubs = subs.map(sub => { return { topic: sub } })
    brokerPublish(this.#broker, TOPIC_REMOVE_SUBSCRIPTION, client.id, mappedSubs, (err) => {
      if (err) {
        errored = true
        cb(err)
      }
    })
  }

  brokerSubscribe (cb) {
    this.#broker.subscribe(
      TOPIC_SUBSCRIPTION_PATTERN,
      this.#onSubMessage,
      cb
    )
  }

  brokerUnsubscribe (cb) {
    this.#broker.unsubscribe(TOPIC_SUBSCRIPTION_PATTERN, this.#onSubMessage, cb)
  }
}

module.exports = BroadcastPersistence
