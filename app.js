require('dotenv').config();
const crypto = require('crypto');
const ws = require('ws');
const { request } = require('undici');

// The minimum delta to look for between market 1's lowest ask and market 2's highest bid.
// If we spot a window with this delta, execute an arbitrage.
const TARGET_DELTA = +0.25;

// The amount to trade
// Use a fixed quantity to avoid exponential growth
const USD_TRADE_QTY = 25;

// The valid receive window for the request by binance us servers
const RECV_WINDOW_MS = 75;

// If a sell order fails to post; try again after a short delay
const RETRY_SELL_INTERVAL_MS = 250;

// Track latest price
const latestOrder = {};

// Lock loop execution while we're working on an order
let LOCK_LOOP = false;

const client = new ws('wss://stream.binance.us:9443/stream?streams=btcusd@bookTicker/btcbusd@bookTicker');
client.on('message', msg => {
  if (LOCK_LOOP === true) return;

  const data = JSON.parse(msg);

  // s = symbol
  // a = lowest ask price
  // A = ask qty
  // b = highest bid price
  // B = bid qty
  const { s, a, A, b, B } = data.data;
  latestOrder[s] = { a, A, b, B };

  const oppositeSymbol = s === 'BTCUSD' ? 'BTCBUSD' : 'BTCUSD';
  const latestOppositeOrder = latestOrder[oppositeSymbol];
  if (!latestOppositeOrder) return;

  const
  ask = +a,
  bid = +b,
  oAsk = +(latestOrder[oppositeSymbol].a),
  oBid = +(latestOrder[oppositeSymbol].b);

  const

  // we're buying the ask and selling the bed if their diff is > delta
  diffA = oBid - ask - TARGET_DELTA,
  diffB = bid - oAsk - TARGET_DELTA;
  // diffA = oBid + MARKET_DIFF - ask,
  // diffB = bid + MARKET_DIFF - oAsk;

  if (diffA <= 0 && diffB <= 0) return;
  else {
    LOCK_LOOP = true;

    if (diffA > diffB) {
      const
      askQty = +A,
      oBidQty = +(latestOrder[oppositeSymbol].B),
      prefQty = Math.floor( USD_TRADE_QTY / oAsk * 10000 ) / 10000;

      const execQty = Math.min( askQty, oBidQty, prefQty );
      executeArbitrage(s, ask, oppositeSymbol, (ask + TARGET_DELTA).toFixed(2), execQty);
    } 
    else {
      const
      oAskQty = +(latestOrder[oppositeSymbol].A),
      bidQty = +B,
      prefQty = Math.floor( USD_TRADE_QTY / ask * 10000 ) / 10000;

      const execQty = Math.min( oAskQty, bidQty, prefQty );
      executeArbitrage(oppositeSymbol, oAsk, s, (oAsk + TARGET_DELTA).toFixed(2), execQty);
    }
  }
  
});

function executeArbitrage(buySymbol, buyPrice, sellSymbol, sellPrice, quantity) {

  const sellCb = () => postOrder({
    side: 'SELL',
    price: sellPrice,
    symbol: sellSymbol,
    quantity,
    callback: () => LOCK_LOOP = false,
    errorHandler: () => setTimeout(sellCb, RETRY_SELL_INTERVAL_MS)
  });

  postOrder({
    side: 'BUY',
    symbol: buySymbol,
    price: buyPrice,
    quantity,
    callback: sellCb
  });

  console.log(`${new Date().toISOString()} > Buy ${buySymbol} @ ${buyPrice}. Sell ${sellSymbol} @ ${sellPrice}. Q: ${quantity}`);
}

/**
 * Take a raw query object (q) and make a query string with
 *  an HMAC signature as required by Binance.US API.
 * 
 * Returns the full query string.
 * @param {*} q
 * @returns 
 */
function makeBinanceQueryString(q) {
  const apiSecret = process.env.API_SECRET;

  const preSigQuery = Object.entries(q)
    .map(x => `${x[0]}=${x[1]}`)
    .join('&');

  const signature = crypto
    .createHmac('sha256', apiSecret)
    .update( preSigQuery )
    .digest('hex');

  const ret = preSigQuery + '&signature=' + signature;
  return ret;
}

/**
 * Post an order of specified side, quantity, and price.
 * Execute callback if successful.
 * 
 * @param {{
 *  side:         'BUY'|'SELL',
 * symbol:        String,
 * quantity:      Number,
 * price:         Number,
 * callback?:     Function,
 * errorHandler?: Function,
 * }} param0 
 */
async function postOrder({
  side,
  symbol,
  quantity,
  price,
  callback,
  errorHandler
}) {
  const q = {
    type:         'LIMIT',
    timeInForce:  'GTC',
    recvWindow:   RECV_WINDOW_MS,
    timestamp:    Date.now(),
    side,
    symbol,
    quantity,
    price
  };

  const query = makeBinanceQueryString(q);

  request(`https://api.binance.us/api/v3/order?${query}`, {
    method: 'POST',
    headers: {
      'X-MBX-APIKEY': process.env.API_KEY
    }
  })
  .then(res => {
    const { body, statusCode } = res;
    if (errorHandler && statusCode !== 200) errorHandler();
    else {
      body.json().then(data => {
        if (statusCode !== 200) handleError(data);
        else callback();
      });
    }
  });
}

/**
 * 
 * @param {*} e 
 */
function handleError(data) {
  if (data.code == -1013) return;

  console.log(`${new Date().toISOString()} > Error! msg: ${ data.msg }`);
  LOCK_LOOP = false;
}