
var amqp = require ('amqp-connection-manager');
var moment = require ('moment');
var cron = require ('node-cron');

const {QUEUE_NAME, INTERVAL} = process.env;
// set up
async function createConnection () {
  conn = amqp.connect ([process.env.AMPQ_CONNECTION_URI]);
  const recevier = await conn.createChannel ({
    json: true,
    setup: channel => {
      console.log (`Attaching consumer to queue[${QUEUE_NAME}]`);

      channel.on ('error', err => {
        console.error (
          `Error consuming message from RabbitMQ[${QUEUE_NAME}]`,
          err
        );
      });

      console.info ('Listening to messages');
      return Promise.all ([
        channel.assertQueue (QUEUE_NAME, {durable: true}),
        channel.prefetch (1),
      ]);
    },
  });

  // Handle RabbitMQ Connection Events
  recevier.on ('connect', () =>
    console.debug (`Connected to RabbitMQ Channel[${QUEUE_NAME}]`)
  );
  recevier.on ('error', err =>
    console.error ('Error occurred during connection', err)
  );
  recevier.on ('close', () =>
    console.info (`Closed RabbitMQ channel[${QUEUE_NAME}]`)
  );

  return recevier;
}

(async () => {
  const conn = await createConnection ();
  conn.addSetup (channel =>
    Promise.all ([
      channel.consume (QUEUE_NAME, async message => {
        await handleMessage (message);
        conn.ack (message);
      }),
    ])
  );
}) ();

let intervalTime;
let bar_num = 1;
let obj = {};
let startTime;
const handleMessage = message => {
  let currentTime = moment ();
  const validMessage = validateMessage (message);
  if (!validMessage) {
    return;
  }
  const {sym, P, Q} = validMessage;
  if (!obj[sym]) {
    obj[sym] = {open: P, high: P, low: P, quantity: Q};
  }
  let {open, high, low, quantity} = obj[sym];

  high = P > high ? ((high = P), (obj[sym].high = P)) : high;
  low = P < low ? ((low = P), (obj[sym].low = P)) : low;
  obj[sym].quantity = quantity + Q;
  if (!startTime) {
    startTime = moment ();
    intervalTime = moment ().add (Number (INTERVAL), 'seconds');
  }

  if (!(currentTime > intervalTime)) {
    console.log ({
      o: open,
      h: high,
      l: low,
      c: 0,
      volume: quantity + Q,
      event: 'ohlc_notify',
      symbol: sym,
      bar_num: bar_num,
    });
  } else {
    console.log ({
      o: open,
      h: high,
      l: low,
      c: P,
      volume: quantity + Q,
      event: 'ohlc_notify',
      symbol: sym,
      bar_num: bar_num,
    });
    bar_num++;
    intervalTime = intervalTime.add (Number (INTERVAL), 'seconds');
  }
};
const validateMessage = message => {
  if (message && message.content) {
    try {
      return JSON.parse (message.content.toString ());
    } catch (err) {
      console.error (`Invalid JSON`);
      return false;
    }
  }
  return false;
};
// increase the bar_num for every interval not depending upon incoming data 
cron.schedule (`*/${INTERVAL} * * * * *`, function () {
  batch = (Number (intervalTime) - Number (startTime)) / 1000;
  if (batch / Number (INTERVAL) !== bar_num) {
    bar_num++;
  }
});
