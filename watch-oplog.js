var MongoOplog = require('mongo-oplog')
    , amqp     = require('amqp')
    , config = {
        mongodb : {
          url  : 'mongodb://localhost:27017/local'
        },
        rabbitmq : {
          host     : 'localhost',
          exchange : 'classroom-activities'
        }
      }
    , queueConnectionReady = false
    , oplog, connection;

oplog      = MongoOplog(config.mongodb.url, 'classroom.*').tail();
connection = amqp.createConnection(config.rabbitmq);

connection.on('ready', function() {
  queueConnectionReady = true;
});

function watch(event) {
  oplog.on(event, function (doc) {
    if (queueConnectionReady) {
      connection.publish(config.rabbitmq.exchange, JSON.stringify(doc), {}, function(err, result) {
        err && console.log(event + ' err:', err);
      });
    }
  });
}

watch('insert');
watch('update');
watch('delete');

oplog.on('error', function (error) {
  console.log('error:', error);
});
 
oplog.on('end', function () {
  console.log('Stream ended');
});
 
oplog.stop(function () {
  console.log('server stopped');
});
