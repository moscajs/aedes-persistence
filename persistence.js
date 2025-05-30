'use strict'

const Packet = require('aedes-packet')
const { CallBackPersistence } = require('./callBackPersistence.js')
const AsyncPersistence = require('./asyncPersistence.js')
const asyncInstanceFactory = (opts) => new AsyncPersistence(opts)
module.exports = (opts) => new CallBackPersistence(asyncInstanceFactory, opts)
module.exports.Packet = Packet
