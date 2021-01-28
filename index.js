require ('dotenv').config ();
var config = require ('./trades.json');
require ('./fsm');
var amqp = require ('amqp-connection-manager');
// set up
var connection = amqp.connect ([process.env.AMPQ_CONNECTION_URI]);
var channelWrapper = connection.createChannel ({
  json: true,
  setup: function (channel) {
    return channel.assertQueue (process.env.QUEUE_NAME, {durable: true});
  },
});
// publish to queue
const publish = async () => {
  try {
    for (let i in config) {
      const {sym, P, Q} = config[i];
      if(!(sym, P, Q)){
          console.info(`some keys missing   ${config[i]}`)
          continue;
      }
      await channelWrapper.sendToQueue (process.env.QUEUE_NAME, config[i]);
    }
  } catch (err) {
    console.error (err);
  }
};
publish ();
