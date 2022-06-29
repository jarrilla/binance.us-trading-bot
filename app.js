require('dotenv').config();
const crypto = require('crypto');
const currencyjs = require('currency.js');

const axios = require('axios');
const i_axios = axios.create({
  baseURL: 'https://api.binance.us',
  timeout: 1000,
  headers: { 'X-MBX-APIKEY': process.env.API_KEY }
});

const SELL_DIFF = +1.5;
const MAX_BUY_DELAY_S = 5;
const MAX_SELL_DELAY_S = 25;

// Small delay after successful sell or buy so funds are available
const TXN_SUCCESS_DELAY = 250;

// After selling, wait a few seconds to avoid being left hanging after riding momentum
const LOOP_DELAY_MS = 1000;

// If sells aren't going through, wait a bit before re-posting to avoid being left hanging after riding momentum
const RECURRING_BUY_DELAY_MS = 1000;

// Delay between check operations
const DELAY_INTERVAL_MS = 250;

// The amount to trade
// Use a fixed quantity to avoid exponential growth
const USD_TRADE_QTY = 25;


// let run_total = currencyjs(0);
loop();

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

function postMarketSell(quantity) {
  const q = {
    symbol:       'BTCUSD',
    type:         'MARKET',
    side:         'SELL',
    recvWindow:   250,
    timestamp:    Date.now(),
    quantity
  };

  const query = makeBinanceQueryString(q);

  console.log(`${ new Date(q.timestamp).toLocaleString() }: MARKET SELL posted. Q: ${quantity}`);

  i_axios.post(`/api/v3/order?${query}`)
    .then(_ => setTimeout(loop, TXN_SUCCESS_DELAY))
    .catch(printError);
}

/**
 * Post an order of specified side, quantity, and price.
 * Execute callback if successful.
 * 
 * @param {{
 *  side:     'BUY'|'SELL',
 * quantity:  Number,
 * price:     Number,
 * callback:  Function
 * }} param0 
 */
async function postOrder({
  side,
  quantity,
  price,
  callback
}) {
  const q = {
    symbol:       'BTCUSD',
    type:         'LIMIT',
    timeInForce:  'GTC',
    recvWindow:   250,
    timestamp:    Date.now(),
    side,
    quantity,
    price
  };

  const query = makeBinanceQueryString(q);

  console.log(`${ new Date(q.timestamp).toLocaleString() }: ${side} posted. Q: ${quantity}, P: ${price}`);

  i_axios.post(`/api/v3/order?${query}`)
    .then(res =>
      setTimeout(() => callback(res),
      side === 'BUY' ? 0 : TXN_SUCCESS_DELAY)
    )
    .catch(printError);
}

/**
 * The main execution loop.
 * 1) Post a BUY order
 * 2) Wait a bit for BUY to be filled, then post SELL
 * 3) If BUY didn't fill, cancel and try again.
 */
async function loop() {
  const _sellCallback = sellQty => async function(res) {
    const { orderId, executedQty } = res.data;

    if (+executedQty == sellQty) {
      console.log(`${new Date().toLocaleString()}: SELL order filled.`);
      setTimeout(loop, LOOP_DELAY_MS);
    } 

    let attempts = Math.ceil( MAX_SELL_DELAY_S * 1000 / DELAY_INTERVAL_MS );

    const _checkSellOrder = async () => {
      if (--attempts >= 0) {
        const { result, qty } = await isOrderFilled(orderId);

        if (result) {
          if (+qty == sellQty) {
            console.log(`${new Date().toLocaleString()}: SELL order filled.`);
            setTimeout(loop, LOOP_DELAY_MS);
          }
        }
        else setTimeout(_checkSellOrder, DELAY_INTERVAL_MS);
      }
      else {

        // If the sell order doesn't fill, take a small loss by posting it again and then keep trading
        cancelOrder(orderId, () => postMarketSell(sellQty), DELAY_INTERVAL_MS)
        // console.log(`${new Date().toLocaleString()}: .`);
      }
    };

    _checkSellOrder();
  };

  // After posting a BUY order we want to wait a bit to see if it fills.
  // If it hasn't filled at all after a delay, cancel the order.
  // If it has been filled partially, keep waiting.
  const _buyCallback = buyQty => async function(res) {
    const { orderId, executedQty, fills } = res.data;

    if (+executedQty === buyQty) {
      console.log(`${new Date().toLocaleString()}: BUY order filled.`);

      postOrder({
        side: 'SELL',
        quantity: executedQty,
        price: (+price + SELL_DIFF),
        callback: _sellCallback(+executedQty)
      });
    }
    else {
      let attempts = Math.ceil( MAX_BUY_DELAY_S * 1000 / DELAY_INTERVAL_MS );

      // Check the status of an order that didn't fill immediately.
      // If it's partially filled, wait indefinitely.
      const _checkBuyOrder = async () => {
        if (--attempts >= 0) {
          const { qty, result } = await isOrderFilled(orderId);

          if (result) {
            if (+qty == buyQty) {
              console.log(`${new Date().toLocaleString()}: BUY order filled.`);

              const newPrice = (+price + SELL_DIFF).toFixed(2);

              postOrder({
                side: 'SELL',
                quantity: qty,
                price: newPrice,
                callback: _sellCallback(qty)
              });
            }
            else {
              attempts++;
            }
          }
          else setTimeout(_checkBuyOrder, DELAY_INTERVAL_MS);
        }
        else {
          console.log(`${new Date().toLocaleString()}: BUY order canceled.`);
          cancelOrder(orderId, loop, RECURRING_BUY_DELAY_MS);
        }
      };

      _checkBuyOrder();
    }
  };

  // Check current book price & post a buy order.
  let price;
  try {
    const res = await i_axios.get(`/api/v3/ticker/bookTicker?symbol=BTCUSD`)
    
    const { bidPrice, askPrice } = res.data;
    let bid = +bidPrice, ask = +askPrice;
    price = bid + (ask - bid)/4;
    price = Math.floor( price * 100 )/100;
  
    const qtyToBuy = Math.floor( USD_TRADE_QTY / (+price) * 1000000 ) / 1000000;
  
    postOrder({
      side: 'BUY',
      quantity: qtyToBuy,
      price,
      callback: _buyCallback(qtyToBuy)
    });
  }
  catch (e) {
    printError(e);
    return loop();
  }
}

/**
 * Cancel a given order.
 * @param {String} orderId the orderID to cancel
 */
function cancelOrder(orderId, callback, delay) {
  const q = {
    symbol: 'BTCUSD',
    orderId,
    timestamp: Date.now()
  };

  const query = makeBinanceQueryString(q);
  i_axios
    .delete(`/api/v3/order?${query}`)
    .then(() => setTimeout(callback, delay))
    .catch(e => {
      const { code } = e?.response?.data || {};

      // code -2011 is 'Unknown order sent' which means the order that we tried to cancel got filled.
      if (code == -2011) setTimeout(callback, delay);
      else printError(e);
    });
}

/**
 * Check if a specified order as filled.
 * @param {String} orderId 
 * @returns True if order was filled, false otherwise.
 */
async function isOrderFilled(orderId) {
  const q = {
    symbol: 'BTCUSD',
    orderId,
    timestamp: Date.now()
  };

  const query = makeBinanceQueryString(q);
  const res = await i_axios
    .get(`/api/v3/order?${query}`)
    .catch(printError);

  const { status, executedQty } = res?.data || {};
  const result = status === 'FILLED';

  return { qty: executedQty, result };
}

/**
 * 
 * @param {*} e 
 */
function printError(e) {
  const { data } = e?.response || {};
  console.log(`${new Date().toLocaleString()}: Error! msg: ${JSON.stringify(data)}`);
}