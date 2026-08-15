"""Robinhood Chain constants and shared ABIs."""

from __future__ import annotations

CHAIN_ID = 4663
CHAIN_NAME = "robinhood"
DEXSCREENER_CHAIN = "robinhood"

# Canonical tokens (docs.robinhood.com/chain/contracts)
WETH = "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"
USDG = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"

# Uniswap deployments on Robinhood Chain
UNI_V2_FACTORY = "0x8bcEaA40B9AcdfAedF85AdF4FF01F5Ad6517937f"
UNI_V2_ROUTER = "0x89e5DB8B5aA49aA85AC63f691524311AEB649eba"
UNI_V3_FACTORY = "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA"
UNI_V3_ROUTER = "0xCaf681a66D020601342297493863E78C959E5cb2"
UNIVERSAL_ROUTER = "0x8876789976dEcBfCbBbe364623C63652db8C0904"
UNI_V4_POOL_MANAGER = "0x8366A39cC670b4001A1121B8f6a443A643e40951"
# Canonical off-chain lens for PoolManager state on Robinhood Chain.
# StateView.getSlot0(poolId) exposes current sqrtPriceX96 without relying on an
# indexer. Deployment is documented by Robinhood Uniswap integrations.
UNI_V4_STATE_VIEW = "0xF3334192D15450CdD385c8B70e03f9A6bD9E673b"
# Deep WETH/USDG V3 0.05% pool, used as an indexer-free ETH/USD oracle for
# alert-time valuation. Resolved from the canonical V3 factory on chain 4663.
WETH_USDG_V3_POOL = "0x69BfaF19C9f377BB306a89aEd9F6B07e2c1a8d9a"

ZERO = "0x0000000000000000000000000000000000000000"

QUOTE_TOKENS = {
    WETH.lower(): {"symbol": "WETH", "decimals": 18, "is_stable": False},
    USDG.lower(): {"symbol": "USDG", "decimals": 6, "is_stable": True},
    ZERO.lower(): {"symbol": "ETH", "decimals": 18, "is_stable": False},
}

KNOWN_ROUTERS = {
    UNI_V2_ROUTER.lower(),
    UNI_V3_ROUTER.lower(),
    UNIVERSAL_ROUTER.lower(),
    UNI_V2_FACTORY.lower(),
    UNI_V3_FACTORY.lower(),
    UNI_V4_POOL_MANAGER.lower(),
    ZERO,
}

# Event topics
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# Sync(uint112,uint112)
SYNC_TOPIC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
# Swap(address,uint256,uint256,uint256,uint256,address) — Uniswap V2
V2_SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
# Swap(address,address,int256,int256,uint160,uint128,int24) — Uniswap V3
V3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
# Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24) — Uniswap V4 PoolManager
V4_SWAP_TOPIC = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
# Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)
V4_INITIALIZE_TOPIC = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
# PairCreated(address,address,address,uint256) — Uniswap V2 factory (unused on RH)
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80129608ed38af698b6c228ce44bb9c8efe4d559091125be063"
# PoolCreated(address,address,uint24,int24,address) — Uniswap V3 factory
# token0 = topics[1], token1 = topics[2], pool = last 20 bytes of data
V3_POOL_CREATED_TOPIC = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"

# Robinhood Chain runs ~10 blocks/sec (~0.1s/block). 24h ≈ 862k blocks.
# Used as the discovery window for "new tokens in the last 24h".
BLOCKS_PER_SECOND = 10
WINDOW_24H_BLOCKS = 24 * 60 * 60 * BLOCKS_PER_SECOND  # 864_000

ERC20_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "totalSupply",
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "symbol",
        "outputs": [{"type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "name",
        "outputs": [{"type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
]

UNI_V2_FACTORY_ABI = [
    {
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
        ],
        "name": "getPair",
        "outputs": [{"name": "pair", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

UNI_V2_PAIR_ABI = [
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getReserves",
        "outputs": [
            {"name": "reserve0", "type": "uint112"},
            {"name": "reserve1", "type": "uint112"},
            {"name": "blockTimestampLast", "type": "uint32"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

UNI_V3_FACTORY_ABI = [
    {
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
            {"name": "fee", "type": "uint24"},
        ],
        "name": "getPool",
        "outputs": [{"name": "pool", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

UNI_V3_POOL_ABI = [
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "fee",
        "outputs": [{"type": "uint24"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96", "type": "uint160"},
            {"name": "tick", "type": "int24"},
            {"name": "observationIndex", "type": "uint16"},
            {"name": "observationCardinality", "type": "uint16"},
            {"name": "observationCardinalityNext", "type": "uint16"},
            {"name": "feeProtocol", "type": "uint8"},
            {"name": "unlocked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "liquidity",
        "outputs": [{"type": "uint128"}],
        "stateMutability": "view",
        "type": "function",
    },
]

UNI_V4_STATE_VIEW_ABI = [
    {
        "inputs": [{"name": "poolId", "type": "bytes32"}],
        "name": "getSlot0",
        "outputs": [
            {"name": "sqrtPriceX96", "type": "uint160"},
            {"name": "tick", "type": "int24"},
            {"name": "protocolFee", "type": "uint24"},
            {"name": "lpFee", "type": "uint24"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

V3_FEE_TIERS = (100, 500, 3000, 10000)

BLOCKSCOUT_BASE = "https://robinhoodchain.blockscout.com"
BLOCKSCOUT_PRO_BASE = "https://api.blockscout.com/4663/api/v2"
DEXSCREENER_API = "https://api.dexscreener.com"
