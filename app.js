require('dotenv').config();
const crypto = require('crypto');
const ws = require('ws');
const { request } = require('undici');

// The minimum delta to look for between market 1's lowest ask and market 2's highest bid.
// If we spot a window with this delta, execute an arbitrage.
const TARGET_DELTA = +0.25;

// minimum trade qty that binance lets us trade
// anything less and it'll be rejected & thus waste of API call
const MIN_USD_TRADE = 11;

// The amount to trade
// Use a fixed quantity to avoid exponential growth
const USD_TRADE_QTY = 25;

// The valid receive window for the request by binance us servers
const RECV_WINDOW_MS = 25;

// delay before retrying a sell attempt
const RETRY_DELAY_MS = 200;

// max attempts before giving up on order
const MAX_ATTEMPTS = 25;

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
  console.log('Resetting loop...');

  LOCK_LOOP = false;
  LATEST_ORDER = {};
};

const client = new ws('wss://stream.binance.us:9443/stream?streams=btcusd@bookTicker/btcbusd@bookTicker');
client.on('message', msg => {
  if (LOCK_LOOP === true) return;

  // lock loop right away
  LOCK_LOOP = true;

  const data = JSON.parse(msg);

  // s = symbol
  // a = lowest ask price
  // A = ask qty
  // b = highest bid price
  // B = bid qty
  const { s, a, A, b, B } = data.data;
  LATEST_ORDER[s] = {
    a: +a,
    A: +A,
    b: +b,
    B: +B
  };

  const oppositeSymbol = s === 'BTCUSD' ? 'BTCBUSD' : 'BTCUSD';
  const latestOppositeOrder = LATEST_ORDER[oppositeSymbol];
  if (!latestOppositeOrder) {
    LOCK_LOOP = false;
    return;
  }

  const
  askPrice = +a,
  bidPrice = +b,
  oAskPrice = LATEST_ORDER[oppositeSymbol].a,
  oBidPrice = LATEST_ORDER[oppositeSymbol].b;

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
 * Attempt to buy LOW and sell HIGH.
 * If buy fails, just reset the loop.
 * 
 * Otherwise, move on to sell.
 * 
 * @param {String} buySymbol 
 * @param {Number} buyPrice 
 * @param {Number} buyQty
 * @param {String} sellSymbol 
 * @param {Number} sellPrice
 */
async function executeArbitrage(buySymbol, buyPrice, buyQty, sellSymbol, sellPrice) {

  const q = {
    side:             'BUY',
    type:             'LIMIT',
    price:            buyPrice,
    symbol:           buySymbol,
    quantity:         buyQty,
    recvWindow:       RECV_WINDOW_MS,
    timeInForce:      'GTC',
    newOrderRespType: 'RESULT'
  };

  const [e, orderRes] = await r_request('/api/v3/order', q, 'POST');
  if (e) handleGenericAPIError(e);
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
  buyQty,
  numAttemptsLeft=MAX_ATTEMPTS
) {
  const q = {
    symbol: buySymbol,
    orderId,
  };

  const [e, orderRes] = await r_request('/api/v3/order', q, 'DELETE');

  // If buy got filled, just market sell
  // TODO: look at API specs and see if a -2011 error code gives us an executedQty
  // we could recover if that's the case
  if (e?.code === -2011) {
    if (side === 'BUY') {
      postMarketSell(sellSymbol, buyQty);
    }

    RESET_LOOP();
  }
  else if (!e) {
    const { executedQty, origQty } = orderRes;

    const sellQty = (side === 'BUY') ?
      Number(executedQty) :
      Math.floor( (Number(origQty) - Number(executedQty)) * 10000 ) / 10000;

    if (sellQty !== 0) postMarketSell(sellSymbol, sellQty);
    else RESET_LOOP();
  }
  else {
    handleGenericAPIError(
      e,
      numAttemptsLeft > 0 ? 
        () => cancelAndMarketSell(orderId, buySymbol, sellSymbol, side, buyQty, numAttemptsLeft-1) :
        undefined
    );
  }
}

/**
 * 
 * @param {String} buySymbol Symbol to buy
 * @param {Number} buyQtyPosted Quantity to buy
 * @param {String} sellSymbol Symbol to sell
 * @param {Number} sellPrice Price to sell at
 * @param {Object} buyOrderRes Result from buy order after latest check
 * @param {Number?} numAttemptsLeft Number of attempts remaining
 * @returns 
 */
async function sellAfterBuy(
  buySymbol,
  buyQtyPosted,
  sellSymbol,
  sellPrice,
  buyOrderRes,
  numAttemptsLeft=MAX_ATTEMPTS
) {
  // check if the order was executed for up to 2s after posting.
  // if 50% or more executed, sell
  // otherwise, just cancel and keep going
  // always market sell, since we're only executing if the cross market ask is more than the bid
  // so a market sell guarantees that we make money
  // in the case that a bid gets sniped, we should lose a lot less than if we leave the order hanging

  const { orderId, executedQty, status } = buyOrderRes;
  const n_executedQty = Number(executedQty);

  if (status === 'FILLED') {
    return limitSell(sellSymbol, buyQtyPosted, sellPrice);
  }
  else if (status === 'PARTIALLY_FILLED' && (n_executedQty >= (buyQtyPosted/2))) {
    await limitSell(sellSymbol, executedQty, sellPrice);

    const marketSellQty = (buyQtyPosted - n_executedQty).toFixed(2);
    return cancelAndMarketSell(orderId, buySymbol, sellSymbol, 'BUY', marketSellQty);
  }
  else if (numAttemptsLeft === 0) {
    return cancelAndMarketSell(orderId, buySymbol, sellSymbol, 'BUY', n_executedQty);
  }

  const [statusErr, statusRes] = await getOrderStatus(buySymbol, orderId);

  // We really want this sell to go through...
  // If there's an error, try again
  if (statusErr) {
    handleGenericAPIError(
      statusErr,
      () => sellAfterBuy(buySymbol, buyQtyPosted, sellSymbol, sellPrice, buyOrderRes)
    );
  }

  // Otherwise, if the order is filled, post a sell right away
  else if (statusRes?.status === 'FILLED') {
    return limitSell(sellSymbol, buyQtyPosted, sellPrice);
  }

  // In all other cases, just recurse with -1 attempt left
  else {
    setTimeout(
      () => sellAfterBuy(buySymbol, buyQtyPosted, sellSymbol, sellPrice, statusRes, numAttemptsLeft-1),
      RETRY_DELAY_MS
    );
  }
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
async function limitSell(symbol, quantity, price, numAttemptsLeft=MAX_ATTEMPTS) {
  const [sellErr, sellRes] = await postLimitSell(symbol, quantity, price);
  if (sellErr) {
    handleGenericAPIError(
      sellErr,
      numAttemptsLeft > 0 ?
        () => limitSell(symbol, quantity, price, numAttemptsLeft-1) :
        () => postMarketSell(symbol, quantity)
    );
  }
  else {
    const { orderId } = sellRes;
    trackSellOrder(quantity, symbol, orderId);
  }
}

/**
 * Attempt to post a sell order.
 * Try up to MAX_ATTEMPTS before reporting an error.
 * 
 * @param {Strin} symbol Symbol to sell
 * @param {Number} quantity Quantity to sell
 * @param {Number} price Price to sell at
 */
async function postLimitSell(symbol, quantity, price) {
  const q = {
    side:         'SELL',
    type:         'LIMIT',
    price,
    symbol,
    quantity,
    timeInForce:  'GTC',
    // newOrderRespType: 'RESULT'
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
 * @param {Number?} numAttemptsLeft 
 * @returns 
 */
async function trackSellOrder(quantity, symbol, orderId, numAttemptsLeft=MAX_ATTEMPTS) {
  const [statusErr, statusRes] = await getOrderStatus(symbol, orderId);
  if (statusErr) {
    handleGenericAPIError(
      statusErr,
      numAttemptsLeft > 0 ?
        () => trackSellOrder(quantity, symbol, orderId, numAttemptsLeft-1) :
        () => postMarketSell(symbol, quantity)
    );
  }
  else if (statusRes?.status === 'FILLED') {
    RESET_LOOP();
  }
  else {
    // if this is last attempt, just market sell
    if (numAttemptsLeft <= 0) cancelAndMarketSell(orderId, symbol, symbol, 'SELL');
    else setTimeout(() => trackSellOrder(quantity, symbol, orderId, numAttemptsLeft-1), RETRY_DELAY_MS);
  }
}

/**
 * Post a market sell for the specified (symbol, quantity)
 * @param {String} symbol 
 * @param {Number} quantity 
 */
async function postMarketSell(symbol, quantity, numAttemptsLeft=MAX_ATTEMPTS) {

  const q = {
    type: 'MARKET',
    side: 'SELL',
    symbol,
    quantity,
  };

  const [e, ] = await r_request('/api/v3/order', q, 'POST');
  if (e) {
    handleGenericAPIError(
      e,
      numAttemptsLeft > 0 ?
        () => postMarketSell(symbol, quantity, numAttemptsLeft-1) :
        undefined
    );
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
  };

  const [e, res] = await r_request('/api/v3/order', q, 'GET');
  if (e) return [e];
  return [null, res];
}

/**
 * Robust function to call the ubinci request method.
 */
async function r_request(endpoint, q, method) {
  q.timestamp = Date.now();
  const query = makeBinanceQueryString(q);
  
  const res = await request(`https://api.binance.us${endpoint}?${query}`, {
    method,
    headers: {
      'X-MBX-APIKEY': process.env.API_KEY
    }
  });

  const data = await res.body.json();
  const { statusCode, headers } = res;
  const ret = { statusCode, headers, ...data };

  if (statusCode != 200) return [ret];
  else return [null, ret];
}

/**
 * 
 * @param {*} error Error to parse
 * @param {Function?} retryCallback Optional retry-callback to execute
 */
function handleGenericAPIError(
  error,
  retryCallback=undefined
) {
  const { status, headers } = error;

  if (status === 403) {
    console.log('Firewall Error.');
    console.log({ status, headers });
    process.exit(1);
  }
  else if (status === 429 || status === 418) {
    const secsToWait = error.headers['Retry-After'];
    setTimeout(retryCallback || RESET_LOOP, secsToWait*1000);
  }
  else if (status !== 400 && status < 500) {
    console.log('Unknown client error.');
    console.log({ status, headers });
    process.exit(2);
  }
  else {
    const { code, msg } = error;
    const errorsToRetry = [ -1000, -1006, -1007, -1013, -1021 ];
    const errorsToReset = [ -2010, -2013 ];
    // const errorsToIgnore = [ -2013 ];
    
    if (errorsToRetry.includes(code)) {
      console.log(msg);
      setTimeout( retryCallback || RESET_LOOP, RETRY_DELAY_MS );
    }
    else if (errorsToReset.includes(code)) {
      console.log(msg);
      RESET_LOOP();
    }
    else {
      console.log({ code, msg, status });
      process.exit(code);
    }
  }
}