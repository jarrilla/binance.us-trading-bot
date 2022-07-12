require('dotenv').config();
const crypto = require('crypto');
const ws = require('ws');
const { request } = require('undici');

// show logs if debugging
const SHOW_LOGS = false;

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
const RECV_WINDOW_MS = 50;

// delay before retrying a sell attempt
const RETRY_DELAY_MS = 100

// max attempts before giving up on order
const MAX_ATTEMPTS = 20;

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
  askPrice = +a,
  bidPrice = +b,
  oAskPrice = +(latestOrder[oppositeSymbol].a),
  oBidPrice = +(latestOrder[oppositeSymbol].b);

  // we're buying the ask and selling the bed if their diff is > delta
  const
  diffA = oBidPrice - askPrice - TARGET_DELTA,
  diffB = bidPrice - oAskPrice - TARGET_DELTA;

  if (diffA <= 0 && diffB <= 0) return;
  else {
    LOCK_LOOP = true;

    const _exec = (buySymbol, buyPrice, sellSymbol) => {
      const buyQty = Math.floor( USD_TRADE_QTY / buyPrice * 10000 ) / 10000;

      if (buyQty * buyPrice < MIN_USD_TRADE) LOCK_LOOP = false;
      else executeArbitrage(buySymbol, buyPrice, buyQty, sellSymbol);
    };

    if (diffA > diffB) _exec(s, askPrice, oppositeSymbol);
    else _exec(oppositeSymbol, oAskPrice, s);
  }
  
});

/**
 * 
 * @param {String} buySymbol 
 * @param {Number | String} buyPrice 
 * @param {String} sellSymbol 
 */
async function executeArbitrage(buySymbol, buyPrice, buyQty, sellSymbol) {

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
  if (e) handleError(e);
  else sellAfterBuy(buySymbol, buyQty, sellSymbol, (buyPrice+TARGET_DELTA).toFixed(2), orderRes);
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
 async function cancelOrder(orderId, msCallback) {
  const q = {
    symbol: 'BTCUSD',
    orderId,
    timestamp: Date.now()
  };

  const [e, ] = await r_request('/api/v3/order', q, 'DELETE');
  if (e.code === -2011) msCallback();
  else if (e) handleError(e);
}

async function sellAfterBuy(buySymbol, buyQty, sellSymbol, sellPrice, orderRes) {
  // check if the order was executed for up to 2s after posting.
  // if 50% or more executed, sell
  // otherwise, just cancel and keep going
  // always market sell, since we're only executing if the cross market ask is more than the bid
  // so a market sell guarantees that we make money
  // in the case that a bid gets sniped, we should lose a lot less than if we leave the order hanging

  const { orderId } = orderRes;
  let numAttepts = MAX_ATTEMPTS;

  // Use this to check an order's status & execute the next step in the logic.
  const _checkOrderStatus = ({ status, executedQty }) => {
    if (status === 'FILLED') _postSellOrder(executedQty);
    else {
      if (+executedQty >= (buyQty/2)) _postMarketSell(executedQty);
      else if (numAttepts-- > 0) setTimeout( () => _checkBuy(), RETRY_DELAY_MS );
      else {
        cancelOrder(orderId, _postMarketSell);
        LOCK_LOOP = false;
      }
    }
  };

  // Use this to sell at specific price
  const _postSellOrder = async (quantity) => {
    const q = {
      side:         'SELL',
      type:         'LIMIT',
      price:        sellPrice,
      symbol:       sellSymbol,
      quantity:     quantity,
      timestamp:    Date.now(),
      recvWindow:   RECV_WINDOW_MS,
      timeInForce:  'GTC',
      newOrderRespType: 'RESULT'
    };
    const [e, ] = await r_request('/api/v3/order', q, 'POST');
    if (e) _postMarketSell();
    else LOCK_LOOP = false;
  };

  // Use this to just market sell.
  const _postMarketSell = async (quantity) => {
    const q = {
      symbol: sellSymbol,
      type: 'MARKET',
      side: 'SELL',
      timestamp: Date.now(),
      quantity
    };
    await r_request('/api/v3/order', q, 'POST');
    LOCK_LOOP = false;
  };

  // Use this to check the buy order's status..
  // Logic is separated b/c we want to check the order first before calling API again
  const _checkBuy = async () => {
    const q = {
      symbol: buySymbol,
      orderId,
      timestamp: Date.now(),
    };
    const [, { executedQty, status }] = await r_request('/api/v3/order', q, 'GET');
    _checkOrderStatus({ executedQty, status });
  };

  _checkOrderStatus(orderRes);
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
  LOCK_LOOP = false;

  if (SHOW_LOGS) console.log(`${new Date().toISOString()} > Error! ${ JSON.stringify(data) }`);
}