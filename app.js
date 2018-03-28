var redis = require('redis')
var redisClient = null
var redisSubscriber = null

function start () {
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
        publishToQueue(reply.queue, reply.message, messageId, reply.correlationId, reply.publishTime, function () {
          console.log('[REDIS] Message requeued ' + messageId)
        })
      } else {
        console.log('[REDIS client] No such message')
      }
    })
  })
}

function publishToQueue (queue, content, messageId, correlationId, publishTime, callback) {
  try {
    redisClient.multi()
    .hdel(messageId + '_clone', 'message')
    .hdel(messageId + '_clone', 'queue')
    .hdel(messageId + '_clone', 'correlationId')
    .hset(messageId, 'message', content)
    .hset(messageId, 'correlationId', correlationId)
    .hset(messageId, 'queue', queue)
    .hset(messageId, 'publishTime', publishTime)
    .zadd(queue, publishTime, messageId)
    .exec(function (err, replies) {
      if (err) {
        console.error('[REDIS publish] error ', err)
      } else {
        callback()
      }
    })
  } catch (e) {
    console.error('[REDIS] publish', e.message)
    callback()
  }
}

start()
