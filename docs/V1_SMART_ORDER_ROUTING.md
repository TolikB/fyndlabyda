# V1 smart order routing

`SmartOrderRouter` is the price, depth, and cost authority for every aggressive
execution instruction. Before it was wired, the planner priced from the top of
book with a slippage pad and checked depth separately, so the price a plan
carried and the depth it assumed came from two different models.

## What routing decides

For each non-post-only leg the planner asks the router for a plan and takes the
price that fills every child of it. The router:

- walks real L2 depth rather than assuming the touch,
- ranks levels by fee- and adverse-selection-adjusted effective price,
- enforces the risk decision's slippage budget and an all-in cost budget that
  adds the venue's taker fee,
- conserves quantity exactly across child routes, and
- refuses anything it cannot fill inside those budgets.

A refusal is a first-class outcome: the plan is blocked as
`execution_route_unavailable` instead of being priced optimistically and
discovered at the venue. A fee rebate is clamped to zero so it can never buy
extra slippage headroom.

## What routing deliberately does not decide

**Routing never reassigns a leg's venue.** In this architecture the venue is part
of the strategy thesis, not an execution detail:

- `SignalLeg.instrument` names the venue, and multi-leg strategies depend on it —
  a cross-exchange lead-lag leg or a dated-basis hedge is venue-specific by
  definition, and moving it would break the relationship the strategy priced.
- Portfolio risk computed venue exposure, correlation, and margin caps for the
  named venue. Substituting a venue after approval would execute against limits
  that were never evaluated.
- `ExecutionPlan` keeps one instruction per leg index, and
  `LiveExecutionApproval` requires that one-to-one shape, so per-leg accounting,
  the OMS, and both paper brokers agree on what a leg is.

So the router optimizes *within* the venue the strategy chose, across that
venue's depth, and reports partial routing by refusing rather than by silently
under-filling. Choosing between venues remains a strategy and universe-selection
decision made before risk approval, where the exposure and correlation limits
that govern it are actually evaluated.

## Emergency flatten

`plan_emergency_flatten` produces a bounded exit plan for every open exposure and
reports residual exposure plus `manual_intervention_required` when a book is
missing. It **plans** an exit; it never submits one. Nothing in the routing path
can place an order.

## Configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `SMART_ORDER_ROUTER_ENABLED` | `true` | With it false the planner falls back to the top-of-book limit and the separate depth check |
| `SMART_ORDER_ROUTER_MAXIMUM_CHILD_ORDERS` | `5` | A route needing more child orders is refused |
| `SMART_ORDER_ROUTER_MAXIMUM_BOOK_PARTICIPATION` | `0.25` | Share of a venue's visible depth one route may consume |

This is an impact guard, not volume participation. Taking a large fraction of
the displayed book is what moves the price the plan was priced against, so each
venue's contribution is capped at its share of visible depth before ranking. A
route that cannot be filled inside that cap is refused rather than allowed to
sweep the book.

Books older than the planner's configured maximum age are excluded from routing,
so a stale venue cannot contribute liquidity to a price.
