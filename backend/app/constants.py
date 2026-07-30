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
# Alias used by migration / Bags docs
POOL_MANAGER = UNI_V4_POOL_MANAGER

# Bags launchpad (docs.bags.fm/robinhood)
BAGS_FACTORY = "0xe8Cc4431adF8b5A847C113EF0c6af9043219Cb37"
BAGS_LENS = "0xC82Db941dAf90B754aecb5F7D14c683dc608d595"
BAGS_V4_HOOK = "0x2380aBf72C17aABAb76480244759AC7E2932EEcC"
BAGS_VAULT = "0x4861446aa7fFd9e67a83cBbAcb1A4B70540B83Aa"
BAGS_STATE_VIEW = "0xF3334192D15450CdD385c8B70e03f9A6bD9E673b"
BAGS_DEPLOY_BLOCK = 7_887_312

# Other RH launchpads (Bitquery / Mobula maps)
HOODFUN_LAUNCHPAD = "0x5fcc1df0dc020cf454e742e9a8ae2554c37a452c"
HOODFUN_LAUNCHPAD_LEGACY = "0x6a63d96ef77ae569fcb85934cf1bd1ec7fe9b33d"
FLAP_LAUNCHPAD = "0x26605f322f7ff986f381bb9a6e3f5dab0beaeb09"
FLAP_VAULT_PORTAL = "0xe9F7AB7DE8FB8756acbB6a1cd13316a43308197B"
CLANKER_LAUNCHPAD = "0xd3f2cc1731b7fd17f28798835c2e02f0a1839a94"
LAUNCHHOOD = "0x62b33a039d289cbda50ebeb72fe4261449e61bcf"
VIRTUALS = "0xd4ccbfa37e2f35611b3042e4096ad7a3459bd007"
KLIK_FINANCE = "0x16cf6788b762ee8969744586ed16fc5705140dd7"
BANKR_BOT = "0xeb7c034704ef8dcd2d32324c1545f62fb4ad0862"
APE_STORE = "0x6e4910ea5a04376032f6564da9a9e4e88b7a87c1"
# Other curve-style pads (NockTerminal registry Jul 2026) — event topics TBD
HOODRICH_CURVE = "0x3c31119db0fd38c46042b6264c67734bd0b2540d"
RECURVE_LAUNCHPAD = "0xd41a03a01369a734a5e22c3d6484b4040ae9acfd"

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

# BagsBondingCurve.Migrated(creator, platformAdmin, token, lpQuote, lpTokens, poolId, sqrtPriceX96)
BAGS_MIGRATED_TOPIC = (
    "0x184d2c0f181283dda427e7607f9f42c0c50de3c6aa3fdee4602f68fde61751cb"
)
# BagsFactory.TokenCreated(token, curve, creator, feeShare, partner, poolId, name, symbol, metadataURI)
BAGS_TOKEN_CREATED_TOPIC = (
    "0x643b3b606052cbadac2f906ad0b462da99eda2a1d4f824d315d7f6edd3e4cced"
)
# hood.fun Graduated(token, pool, liquidityEth, liquidityTokens)
HOODFUN_GRADUATED_TOPIC = (
    "0x18a56450d3c666e2bae9e0829fcada82a9ab0deef6e33c2496752c88d4155c9d"
)
# hood.fun TokenCreated(token, creator, name, symbol, metadataURI, virtualEth, curveSupply)
HOODFUN_TOKEN_CREATED_TOPIC = (
    "0x979cee093a93828d2e8b673315ae1acdbd57ec336874aeba054d347b48b9e5d1"
)
# Flap.sh Portal.LaunchedToDEX(token, pool, amount, eth) — all args non-indexed
FLAP_LAUNCHED_TO_DEX_TOPIC = (
    "0x6e4f47630b8745b8cacbd44f42a8a33e7eea7cc08ef22fc7630f4f385784ff7d"
)

BAGS_LENS_ABI = [
    {
        "inputs": [{"name": "token", "type": "address"}],
        "name": "getTokenState",
        "outputs": [
            {
                "components": [
                    {"name": "exists", "type": "bool"},
                    {"name": "migrated", "type": "bool"},
                    {"name": "curve", "type": "address"},
                    {"name": "feeShare", "type": "address"},
                    {"name": "poolId", "type": "bytes32"},
                    {"name": "thresholdQuote", "type": "uint256"},
                    {"name": "realQuoteReserves", "type": "uint256"},
                    {"name": "realTokenReserves", "type": "uint256"},
                    {"name": "virtualTokenReserves", "type": "uint256"},
                    {"name": "virtualQuoteReserves", "type": "uint256"},
                    {"name": "priceQuotePerToken", "type": "uint256"},
                    {"name": "bondingProgressPct", "type": "uint256"},
                    {"name": "totalRaised", "type": "uint256"},
                ],
                "type": "tuple",
            }
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

BAGS_FACTORY_ABI = [
    {
        "inputs": [{"name": "token", "type": "address"}],
        "name": "curveForToken",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "poolId", "type": "bytes32"}],
        "name": "tokenForPoolId",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

HOODFUN_ABI = [
    {
        "inputs": [{"name": "token", "type": "address"}],
        "name": "curves",
        "outputs": [
            {"name": "virtualEth", "type": "uint256"},
            {"name": "virtualTokens", "type": "uint256"},
            {"name": "realEth", "type": "uint256"},
            {"name": "realTokens", "type": "uint256"},
            {"name": "tradeFeeBps", "type": "uint256"},
            {"name": "readyToGraduate", "type": "bool"},
            {"name": "graduated", "type": "bool"},
            {"name": "migrated", "type": "bool"},
            {"name": "creator", "type": "address"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "token", "type": "address"}],
        "name": "isHoodToken",
        "outputs": [{"type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
]

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

V3_FEE_TIERS = (100, 500, 3000, 10000)

BLOCKSCOUT_BASE = "https://robinhoodchain.blockscout.com"
BLOCKSCOUT_PRO_BASE = "https://api.blockscout.com/4663/api/v2"
DEXSCREENER_API = "https://api.dexscreener.com"
