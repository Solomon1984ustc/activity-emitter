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

oplog.on('insert', function (doc) {
  connection.publish(config.rabbitmq.exchange, JSON.stringify(doc), {}, function(err, result) {
    err && console.log('err:', err);
  });
});
 
oplog.on('update', function (doc) {
  connection.publish(config.rabbitmq.exchange, JSON.stringify(doc), {}, function(err, result) {
    err && console.log('err:', err);
  });
});
 
oplog.on('delete', function (doc) {
  connection.publish(config.rabbitmq.exchange, JSON.stringify(doc), {}, function(err, result) {
    err && console.log('err:', err);
  });
});
 
oplog.on('error', function (error) {
  console.log('error:', error);
});
 
oplog.on('end', function () {
  console.log('Stream ended');
});
 
oplog.stop(function () {
  console.log('server stopped');
});
