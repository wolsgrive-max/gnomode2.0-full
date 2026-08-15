"""V2/V3/V4 entry-mcap helpers (pre-swap spot + fill)."""

from __future__ import annotations

from app.replay import (
    entry_mcap_usd,
    v2_reserves_before_swap,
    v3_mcap_from_swap,
    v3_price_from_sqrt,
    v3_sqrt_before_swap,
)


def test_v3_sqrt_before_fraggle_buy():
    """Real Fraggle first buy: 2 ETH in → post mcap ~15.8k, pre much lower."""
    amount0 = int(2e18)
    amount1 = int(-593586075.6635233 * 1e18)
    sqrt_after = 874526815064390129495953266040919
    liquidity = 36819258015569838458222
    before = v3_sqrt_before_swap(sqrt_after, liquidity, amount0, amount1)
    assert before is not None
    assert before > sqrt_after  # zeroForOne → sqrt drops

    quote_usd = 1922.0
    supply = 1_000_000_000.0
    mcap_after = (
        v3_price_from_sqrt(sqrt_after, False, 18, 18) * quote_usd * supply
    )
    mcap_before, _ = v3_mcap_from_swap(
        amount0=amount0,
        amount1=amount1,
        sqrt_after=sqrt_after,
        liquidity=liquidity,
        token_is_token0=False,
        decimals=18,
        quote_decimals=18,
        quote_usd=quote_usd,
        supply_tokens=supply,
    )
    assert 14_000 < mcap_after < 17_000
    assert 1_500 < mcap_before < 5_000
    assert mcap_before < mcap_after / 2


def test_entry_mcap_prefers_fill_like_gmgn():
    """Fill (USD spent / tokens × supply) ≈ GMGN ~6k for Fraggle buy."""
    quote_usd = 1922.0
    supply = 1_000_000_000.0
    quote_in = int(2e18)
    token_out = int(593586075.6635233 * 1e18)
    spot_pre = 2500.0
    fill = entry_mcap_usd(
        quote_in_raw=quote_in,
        token_out_raw=token_out,
        quote_decimals=18,
        token_decimals=18,
        quote_usd=quote_usd,
        supply_tokens=supply,
        spot_mcap=spot_pre,
    )
    assert 5_500 < fill < 7_500
    # Without amounts, fall back to spot.
    assert (
        entry_mcap_usd(
            quote_in_raw=0,
            token_out_raw=0,
            quote_decimals=18,
            token_decimals=18,
            quote_usd=quote_usd,
            supply_tokens=supply,
            spot_mcap=spot_pre,
        )
        == spot_pre
    )


def test_v2_reserves_before_swap():
    # Post reserves after buying token1 with token0
    post0, post1 = 1_000_000, 500_000
    a0_in, a1_in, a0_out, a1_out = 100, 0, 0, 50
    pre = v2_reserves_before_swap(post0, post1, a0_in, a1_in, a0_out, a1_out)
    assert pre == (999_900, 500_050)


def test_v3_sqrt_before_rejects_bad_liquidity():
    assert v3_sqrt_before_swap(1000, 0, 1, -1) is None
    assert v3_sqrt_before_swap(0, 1000, 1, -1) is None


def test_v4_swapper_amounts_negated_for_before():
    """V4 swapper deltas are opposite of pool deltas — negate before reverse."""
    amount0_pool = int(2e18)
    amount1_pool = int(-593586075.6635233 * 1e18)
    sqrt_after = 874526815064390129495953266040919
    liquidity = 36819258015569838458222
    before_pool = v3_sqrt_before_swap(sqrt_after, liquidity, amount0_pool, amount1_pool)
    swapper0, swapper1 = -amount0_pool, -amount1_pool
    before_v4 = v3_sqrt_before_swap(sqrt_after, liquidity, -swapper0, -swapper1)
    assert before_pool is not None
    assert before_v4 == before_pool
