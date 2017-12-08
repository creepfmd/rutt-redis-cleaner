var amqp = require('amqplib/callback_api')
var redis = require('redis')
var redisClient = null
var redisSubscriber = null
var amqpConn = null
var pubChannel = null

function start () {
  amqpConn = null
  pubChannel = null
  amqp.connect(process.env.AMQP_URL + '?heartbeat=60', function (err, conn) {
    if (err) {
      console.error('[AMQP]', err.message)
      return setTimeout(start, 500)
    }
    conn.on('error', function (err) {
      if (err.message !== 'Connection closing') {
        console.error('[AMQP] conn error', err.message)
      }
    })
    conn.on('close', function () {
      console.error('[AMQP] reconnecting')
      return setTimeout(start, 500)
    })
    console.log('[AMQP] connected')
    amqpConn = conn
    redisSubscriber = redis.createClient('redis://redis')
    redisClient = redis.createClient('redis://redis')
    redisClient.on('error', function (err) {
      console.log('[REDIS client] ' + err)
    })
    redisSubscriber.on('error', function (err) {
      console.log('[REDIS subscriber] ' + err)
    })
    redisSubscriber.psubscribe('__keyevent@0__:expired')
    redisSubscriber.on('pmessage', function (pattern, channel, messageId) {
      console.info('[REDIS subscriber] Message ' + messageId + ' expired')
      redisClient.hgetall(messageId + '_clone', function (err, reply) {
        if (err) {
          console.log('[REDIS client] ' + err)
        }
        if (reply) {
          publishToQueue(reply.queue, reply.message, messageId, reply.correlationId, function () {
            console.log('[REDIS] Message requeued ' + messageId)
            redisClient.multi()
              .hdel(messageId + '_clone', 'message')
              .hdel(messageId + '_clone', 'queue')
              .hdel(messageId + '_clone', 'correlationId')
              .exec(function (err, replies) {
                if (err) {
                  console.log('[REDIS client] ' + err)
                }
              })
          })
        } else {
          console.log('[REDIS client] No such message')
        }
      })
    })
  })
}

function publishToQueue (queue, content, messageId, correlationId, callback) {
  try {
    if (!pubChannel) {
      amqpConn.createConfirmChannel(function (err, ch) {
        if (err) {
          console.error('[AMQP]', err.message)
          return setTimeout(start, 500)
        }
        ch.on('error', function (err) {
          console.error('[AMQP] channel error', err.message)
        })
        ch.on('close', function () {
          console.log('[AMQP] channel closed')
          pubChannel = null
          return setTimeout(start, 500)
        })
        pubChannel = ch
        pubChannel.sendToQueue(queue, Buffer.from(content), { persistent: true, messageId: messageId, correlationId: correlationId },
                          function (err, ok) {
                            if (err !== null) {
                              console.error('[AMQP] publish', err)
                            } else {
                              callback()
                            }
                          })
      })
    } else {
      pubChannel.sendToQueue(queue, Buffer.from(content), { persistent: true, messageId: messageId, correlationId: correlationId },
                      function (err, ok) {
                        if (err !== null) {
                          console.error('[AMQP] publish', err)
                        } else {
                          callback()
                        }
                      })
    }
  } catch (e) {
    console.error('[AMQP] publish', e.message)
    callback()
  }
}

start()
