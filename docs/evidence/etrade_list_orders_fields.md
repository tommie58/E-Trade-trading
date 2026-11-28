# E*TRADE List Orders fields used to match a lost stock stop

The lost-stop path does not treat `clientOrderId` as proof an order is absent.
E*TRADE documents that the id is not returned on any API response. The match
uses fields `parse_open_orders` already reads from a real List Orders payload:
`orderId` when we have one, otherwise symbol, `orderAction`, `priceType`,
`orderedQuantity`, and `stopPrice` when that price is present.

## 1. API documentation

Source: E*TRADE Order API v1, List Orders
(`GET /v1/accounts/{accountIdKey}/orders`),
https://apisb.etrade.com/docs/api/order/api-order-v1.html
(retrieved 2026-11-28).

The List Orders success model is `OrdersResponse` → `Order` → `OrderDetail`
→ `Instrument`. Documented response fields used here:

| Object | Field | In the List Orders response |
| --- | --- | --- |
| Order | `orderId` | yes |
| OrderDetail | `status` | yes |
| OrderDetail | `priceType` | yes. Equity stops use `STOP` and `STOP_LIMIT` |
| OrderDetail | `stopPrice` | yes, "the designated boundary price for a stop order" |
| Instrument | `orderAction` | yes. A long exit is `SELL` |
| Instrument | `orderedQuantity` | yes |
| Instrument / Product | `symbol` | yes, via `Product.symbol` |

`clientOrderId` is documented on the **place-order request**, not on
`Order` or `OrderDetail`. The place-order description says: "This field
does not appear in any API responses." It is therefore not a List Orders
lookup key. An order with no `clientOrderId` is not evidence that the stop
is gone.

## 2. Parser

`parse_open_orders` in `main_bot.py` (the `reconciliation` bundle) copies
those response fields into the dict the rest of the bot uses:

- `orderId` → `order_id`
- `OrderDetail.status` → `status` (blank defaults to `OPEN`)
- `OrderDetail.priceType` → `price_type`
- `OrderDetail.stopPrice` → `stop_price` (only when positive)
- `Instrument.orderAction` → `order_action`
- `Instrument.orderedQuantity` (else `quantity`) → `ordered`
- `Product.symbol` → `symbol`
- `clientOrderId` is copied when present → `client_order_id`, and is `""`
  when the response omits it

The lost-stop match (`classify_lost_stop`) uses `order_id` when that id is
known. Otherwise it requires symbol, `order_action` `SELL`, `price_type`
`STOP` or `STOP_LIMIT`, a working status, `ordered` equal to the quantity
sent, `stop_price` within ±0.005, and `placedTime` at or after the guard
start. `placedTime` is read in `parse_open_orders` as `placed_ts`. A blank
`client_order_id` is not a miss. A missing match field is a possible stop.
Exactly one definitive match is cancelled. More than one candidate is page
only, not a cancel-all. Market closes and limit take-profits are excluded.

## 3. Fixtures in this repo

There is no captured production List Orders body in the tree. Tests build
the same JSON shape the parser expects. No account id or account number is
included in these examples.

From `tests/test_59922_lost_stop_reply.py` (order builder used across the
stop-guard tests): `orderId`, `OrderDetail.status`, `OrderDetail.priceType`,
`OrderDetail.stopPrice`, `Instrument.orderAction`, `Instrument.orderedQuantity`,
`Instrument.Product.symbol`. `clientOrderId` is optional on that builder.

`tests/test_inferred_flat_59917.py` has the same shape for a STOP
(`priceType` `STOP`, `stopPrice`, `orderedQuantity`) and for a MARKET order.
Neither fixture contains an account id.
