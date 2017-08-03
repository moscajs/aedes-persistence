'use strict'

var util = require('util')
var Qlobber = require('qlobber').Qlobber

function QlobberSub (options) {
  Qlobber.call(this, options)
}

util.inherits(QlobberSub, Qlobber)

QlobberSub.prototype._initial_value = function (val) {
  var topicMap = new Map().set(val.topic, val.qos)
  return new Map().set(val.clientId, topicMap)
}

QlobberSub.prototype._add_value = function (clientMap, val) {
  var topicMap = clientMap.get(val.clientId)
  if (!topicMap) {
    topicMap = new Map()
    clientMap.set(val.clientId, topicMap)
  }
  topicMap.set(val.topic, val.qos)
}

QlobberSub.prototype._add_values = function (dest, clientMap) {
  for (var clientIdAndTopicMap of clientMap) {
    for (var topicAndQos of clientIdAndTopicMap[1]) {
      dest.push({
        clientId: clientIdAndTopicMap[0],
        topic: topicAndQos[0],
        qos: topicAndQos[1]
      })
    }
  }
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
  return topicMap && topicMap.has(val.topic)
}

QlobberSub.prototype.match = function (topic) {
  return this._match([], 0, topic.split(this._separator), this._trie)
}

module.exports = QlobberSub
