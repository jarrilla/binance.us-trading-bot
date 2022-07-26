require('dotenv').config();
const crypto = require('crypto');
const ws = require('ws');
const { request } = require('undici');

// The minimum delta to look for between market 1's lowest ask and market 2's highest bid.
// If we spot a window with this delta, execute an arbitrage.
const TARGET_DELTA = +0.25;

// minimum trade qty that binance lets us trade
// anything less and it'll be rejected & thus waste of API call
const MIN_USD_TRADE = 10;

// The amount to trade
// Use a fixed quantity to avoid exponential growth
const USD_TRADE_QTY = 25;

// The valid receive window for the request by binance us servers
const RECV_WINDOW_MS = 25;

// delay before retrying a sell attempt
const RETRY_DELAY_MS = 100

// max attempts before giving up on order
const MAX_ATTEMPTS = 35;

// Track latest price
let LATEST_ORDER = {};

// Lock loop execution while we're working on an order
let LOCK_LOOP = false;

/**
 * Previously we were just unlocking the loop but LATEST_ORDER was stale.
 * So we were either left stuck on a lowball buy
 * Or executing a bad order.
 * 
 * This way, we delay our loop a bit b/c we have to find a new pair,
 * but we guarantee that we're only executing good opportunities.
 */
function RESET_LOOP() {
  LOCK_LOOP = false;
  LATEST_ORDER = {};
};

const client = new ws('wss://stream.binance.us:9443/stream?streams=btcusd@bookTicker/btcbusd@bookTicker');
client.on('message', msg => {
  if (LOCK_LOOP === true) return;

  LOCK_LOOP = true;

  const data = JSON.parse(msg);

  // s = symbol
  // a = lowest ask price
  // A = ask qty
  // b = highest bid price
  // B = bid qty
  const { s, a, A, b, B } = data.data;
  LATEST_ORDER[s] = { a, A, b, B };

  const oppositeSymbol = s === 'BTCUSD' ? 'BTCBUSD' : 'BTCUSD';
  const latestOppositeOrder = LATEST_ORDER[oppositeSymbol];
  if (!latestOppositeOrder) {
    LOCK_LOOP = false;
    return;
  }

  const
  askPrice = +a,
  bidPrice = +b,
  oAskPrice = +(LATEST_ORDER[oppositeSymbol].a),
  oBidPrice = +(LATEST_ORDER[oppositeSymbol].b);

  // we're buying the ask and selling the bed if their diff is > delta
  const
  diffA = oBidPrice - askPrice - TARGET_DELTA,
  diffB = bidPrice - oAskPrice - TARGET_DELTA;

  if (diffA <= 0 && diffB <= 0) {
    LOCK_LOOP = false;
    return;
  }
  else {
    if (diffA > diffB) {
      const buyQty = Math.floor( USD_TRADE_QTY / askPrice * 10000 ) / 10000;

      if ( buyQty * askPrice < MIN_USD_TRADE ) RESET_LOOP();
      else {
        executeArbitrage(s, askPrice, buyQty, oppositeSymbol, oBidPrice);
      }
    }
    else {
      const buyQty = Math.floor( USD_TRADE_QTY / oAskPrice * 10000 ) / 10000;

      if ( buyQty * oAskPrice < MIN_USD_TRADE ) RESET_LOOP();
      else {
        executeArbitrage(oppositeSymbol, oAskPrice, buyQty, s, bidPrice);
      }
    }
  }
  
});

/**
 * BUY low SELL high
 * @param {String} buySymbol 
 * @param {Number | String} buyPrice 
 * @param {String} sellSymbol 
 */
async function executeArbitrage(buySymbol, buyPrice, buyQty, sellSymbol, sellPrice) {

  const q = {
    side:         'BUY',
    type:         'LIMIT',
    price:        buyPrice,
    symbol:       buySymbol,
    quantity:     buyQty,
    timestamp:    Date.now(),
    recvWindow:   RECV_WINDOW_MS,
    timeInForce:  'GTC',
    newOrderRespType: 'RESULT'
  };

  const [e, orderRes] = await r_request('/api/v3/order', q, 'POST');
  if (e) {
    handleError(e);
    return [e];
  }
  else {
    // sellAfterBuy(buySymbol, buyQty, sellSymbol, (+buyPrice+TARGET_DELTA).toFixed(2), orderRes);
    sellAfterBuy(buySymbol, buyQty, sellSymbol, sellPrice, orderRes);
  }
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
 * Cancel a given order.
 * @param {String} orderId the orderID to cancel
 */
 async function cancelAndMarketSell(
  orderId,
  buySymbol,
  sellSymbol,
  side,
  buyQty
) {
  const q = {
    symbol: buySymbol,
    orderId,
    timestamp: Date.now()
  };

  const [e, orderRes] = await r_request('/api/v3/order', q, 'DELETE');
  if (e?.code === -2011) {

    // if BUY got filled, just market sell
    // TODO: we could actually still recover and potentially arbitrage here
    if (side === 'BUY') {
      marketSell(sellSymbol, buyQty);
    }

    return [null, ];
  }
  else if (!e) {
    const { executedQty, origQty } = orderRes;

    let sellQty;
    if (side === 'BUY') sellQty = executedQty;
    else sellQty = Math.floor( (Number(origQty) - Number(executedQty)) * 10000 ) / 10000;

    if (sellQty != 0) {
      marketSell(sellSymbol, sellQty);
    }
    else {
      RESET_LOOP();
    }
    return [null, ];
  }
  else {
    handleError(e);
    return [e];
  }
}

async function sellAfterBuy(buySymbol, buyQtyPosted, sellSymbol, sellPrice, orderRes, numAttepts=MAX_ATTEMPTS) {
  // check if the order was executed for up to 2s after posting.
  // if 50% or more executed, sell
  // otherwise, just cancel and keep going
  // always market sell, since we're only executing if the cross market ask is more than the bid
  // so a market sell guarantees that we make money
  // in the case that a bid gets sniped, we should lose a lot less than if we leave the order hanging

  const { orderId, executedQty, status } = orderRes;

  if (status === 'FILLED') {
    return limitSell(sellSymbol, buyQtyPosted, sellPrice);
  }
  else if (status === 'PARTIALLY_FILLED' && (+(executedQty) >= (buyQtyPosted/2))) {
    await limitSell(sellSymbol, executedQty, sellPrice);

    const mSellQty = (+buyQtyPosted - (+executedQty)).toFixed(2);
    return cancelAndMarketSell(orderId, buySymbol, sellSymbol, 'BUY', mSellQty);
  }
  else if (numAttepts === 0) {
    return cancelAndMarketSell(orderId, buySymbol, sellSymbol, 'BUY', executedQty);
  }

  const [statusErr, statusRes] = await getOrderStatus(buySymbol, orderId);
  if (statusErr) {
    handleError(statusErr);
    return [statusErr];
  }

  setTimeout(
    () => sellAfterBuy(buySymbol, buyQtyPosted, sellSymbol, sellPrice, statusRes, numAttepts-1),
    RETRY_DELAY_MS
  );
}

/**
 * Attempt to post a LIMIT sell order.
 * If it fails to post, retry a few times.
 * After order is posted, check on it for a few seconds
 * If it never filled, cancel and market sell.
 * @param {String} symbol
 * @param {String | NUmber} quantity 
 * @param {String | Number} price 
 * @returns 
 */
async function limitSell(symbol, quantity, price, numAttepts=MAX_ATTEMPTS) {
  const [sellErr, sellRes] = await postSellOrder(symbol, quantity, price);
  if (sellErr) {
    if (numAttepts > 0) setTimeout(() => limitSell(symbol, quantity, price, numAttepts-1), RETRY_DELAY_MS);
    else handleError(sellErr);
  }
  else {
    const { orderId } = sellRes;
    trackSellOrder(quantity, symbol, orderId);

    RESET_LOOP();
  }
}

async function postSellOrder(symbol, quantity, price) {
  const q = {
    side:         'SELL',
    type:         'LIMIT',
    price,
    symbol,
    quantity,
    timestamp:    Date.now(),
    // recvWindow:   RECV_WINDOW_MS,
    timeInForce:  'GTC',
    newOrderRespType: 'RESULT'
  };

  const [e, orderRes] = await r_request('/api/v3/order', q, 'POST');
  if (e) return [e];
  else return [null, orderRes];
}

/**
 * Track a sell order for up to a few seconds.
 * If it's filled before, un-track.
 * Otherwise, cancel and market sell.
 * 
 * The delay should be short so we don't lose too much by market selling.
 * @param {String | Number} quantity 
 * @param {String} symbol 
 * @param {String} orderId 
 * @param {Number?} numAttepts 
 * @returns 
 */
async function trackSellOrder(quantity, symbol, orderId, numAttepts=MAX_ATTEMPTS) {
  const [statusErr, statusRes] = await getOrderStatus(symbol, orderId);

  if (statusErr) return marketSell(symbol, quantity);
  const { status } = statusRes;

  if (status === 'FILLED') {
    return;
  }
  else {

    // if this is last attempt, just market sell
    if (numAttepts === 0) cancelAndMarketSell(orderId, symbol, symbol, 'SELL');
    else setTimeout(() => trackSellOrder(quantity, symbol, orderId, numAttepts-1), RETRY_DELAY_MS);
  }
}

/**
 * Post a market sell for the specified (symbol, quantity)
 * @param {String} symbol 
 * @param {Number | String} quantity 
 */
async function marketSell(symbol, quantity) {

  const q = {
    type: 'MARKET',
    side: 'SELL',
    symbol,
    quantity,
    timestamp: Date.now(),
  };

  const [e, r] = await r_request('/api/v3/order', q, 'POST');
  if (e) {
    handleError(e);
    return [e];
  }
  else {
    RESET_LOOP();
    return [null, ];
  }
}

/**
 * Check order status for given (symbol, order)
 * @param {String} symbol 
 * @param {String} orderId 
 * @returns 
 */
async function getOrderStatus(symbol, orderId) {
  const q = {
    symbol,
    orderId,
    timestamp: Date.now()
  };

  const [e, res] = await r_request('/api/v3/order', q, 'GET');
  if (e) return [e];
  return [null, res];
}

/**
 * Robust function to call the ubinci request method.
 */
async function r_request(endpoint, q, method) {
  const query = makeBinanceQueryString(q);
  
  const res = await request(`https://api.binance.us${endpoint}?${query}`, {
    method,
    headers: {
      'X-MBX-APIKEY': process.env.API_KEY
    }
  });

  const data = await res.body.json();

  if (res.statusCode != 200) return [data];
  else return [null, data];
}

/**
 * Generic error handler.
 * Just print and keep going.
 * @param {*} e 
 */
function handleError(data) {
  RESET_LOOP();
}