import pandas as pd
import numpy as np
import os
import yaml
from datetime import datetime
import glob
from scipy.optimize import minimize

# ----------------------------- CONFIG -----------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(SCRIPT_DIR, 'config.yaml'), 'r') as f:
    config = yaml.safe_load(f)

PRICING_METHOD = config['pricing_method']
CONTRIBUTION = config.get('contribution', 0)
MODEL = config.get('model', 'custom').lower()

DATA_DIR = os.path.join(SCRIPT_DIR, 'Data')
HOLDINGS_FILE = os.path.join(SCRIPT_DIR, 'holdings.csv')
OUTPUT_FILE = os.path.join(SCRIPT_DIR, f'port_rebal_{datetime.now().strftime("%Y%m%d")}.xlsx')

# ----------------------------- HELPER ---------------------------
def _normalize(w_dict: dict):
    total = sum(w_dict.values())
    if abs(total - 1.0) > 1e-8:
        largest = max(w_dict, key=w_dict.get)
        w_dict[largest] = round(w_dict[largest] + (1.0 - total), 6)

def build_symbol_file_map() -> dict:
    """
    Scan all CSV files in DATA_DIR, read the SYMBOL column from each,
    and return a mapping of {SYMBOL: filepath}.
    Handles UUID-named files like query-2f4ad9a6-...-011b3ebcf122.csv.
    """
    pattern = os.path.join(DATA_DIR, '*.csv')
    files = glob.glob(pattern)
    symbol_map = {}
    for fp in files:
        try:
            df = pd.read_csv(fp, nrows=1)
            df.columns = df.columns.str.strip()
            if 'SYMBOL' in df.columns:
                symbol = df['SYMBOL'].iloc[0].upper()
                symbol_map[symbol] = fp
        except Exception as e:
            print(f"  Warning: could not read {fp}: {e}")
    return symbol_map

SYMBOL_FILE_MAP = build_symbol_file_map()
print(f"Found data files for: {', '.join(sorted(SYMBOL_FILE_MAP.keys()))}")

# ----------------------------- DYNAMIC WEIGHTS -----------------------------
def calculate_market_cap_weights() -> dict:
    totals = {}
    print("\nCalculating market-cap weights...")
    for ticker, fp in SYMBOL_FILE_MAP.items():
        if ticker == 'USDC': continue
        df = pd.read_csv(fp)
        df.columns = df.columns.str.strip()
        if 'PRICE_USD' not in df.columns or 'CIRCULATING_TOKENS' not in df.columns:
            print(f"  Warning: {ticker} missing required columns, skipping")
            print(f"    Available columns: {list(df.columns)}")
            continue
        mc = (df['PRICE_USD'] * df['CIRCULATING_TOKENS']).mean()
        totals[ticker] = mc
        print(f"  {ticker}: ${mc:,.0f}")
    if not totals:
        raise ValueError("No assets with market cap data found. Cannot calculate market cap weights.")
    total = sum(totals.values())
    weights = {t: round(v/total, 6) for t, v in totals.items()}
    weights['USDC'] = 0.0
    _normalize(weights)
    return weights

def calculate_volume_weights() -> dict:
    totals = {}
    print("\nCalculating volume-weighted weights...")
    for ticker, fp in SYMBOL_FILE_MAP.items():
        if ticker == 'USDC': continue
        df = pd.read_csv(fp)
        df.columns = df.columns.str.strip()
        if 'CEX_TRADING_VOLUME_24H_USD' not in df.columns:
            print(f"  Warning: {ticker} missing CEX_TRADING_VOLUME_24H_USD column, skipping")
            print(f"    Available columns: {list(df.columns)}")
            continue
        vol = df['CEX_TRADING_VOLUME_24H_USD'].mean()
        totals[ticker] = vol
        print(f"  {ticker}: ${vol:,.0f}")
    if not totals:
        raise ValueError("No assets with trading volume data found. Cannot calculate volume weights.")
    total = sum(totals.values())
    weights = {t: round(v/total, 6) for t, v in totals.items()}
    weights['USDC'] = 0.0
    _normalize(weights)
    return weights

def calculate_risk_parity_weights(active_assets: list) -> dict:
    if not active_assets:
        return {}
    print("\nCalculating risk-parity weights (Equal Risk Contribution)...")
    prices = {}
    for ticker in active_assets:
        t = ticker.upper()
        if t not in SYMBOL_FILE_MAP:
            print(f"  Warning: No data file for {t}, skipping from risk-parity")
            continue
        df = pd.read_csv(SYMBOL_FILE_MAP[t])
        df.columns = df.columns.str.strip()
        if 'DATE' not in df.columns or 'PRICE_USD' not in df.columns:
            raise ValueError(f"{t} missing required columns (DATE or PRICE_USD). Available: {list(df.columns)}")
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df.sort_values('DATE')
        prices[ticker] = df.set_index('DATE')['PRICE_USD']

    included_assets = list(prices.keys())
    if not included_assets:
        raise ValueError("No assets with price data found for risk-parity")

    price_df = pd.DataFrame(prices).dropna()
    if len(price_df) < 30:
        raise ValueError("Not enough overlapping price data for risk-parity")

    returns = price_df.pct_change().dropna()
    cov = returns.cov() * 252

    def objective(w):
        port_var = w @ cov.values @ w
        port_std = np.sqrt(port_var)
        mrc = cov.values @ w / port_std
        rc = w * mrc
        target = rc.mean()
        return np.sum((rc - target)**2)

    n = len(included_assets)
    x0 = np.ones(n) / n
    bounds = [(0, 1) for _ in range(n)]
    constraints = {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}
    res = minimize(objective, x0, method='SLSQP', bounds=bounds, constraints=constraints,
                   options={'maxiter': 1000})

    if not res.success:
        print("Warning: Risk-parity optimization did not converge perfectly")

    weights = dict(zip(included_assets, np.round(res.x, 6)))
    weights['USDC'] = 0.0
    _normalize(weights)
    return weights

# ----------------------------- DETERMINE WHICH ASSETS TO INCLUDE -----------------------------
# Load holdings first so we know which assets exist in the portfolio
holdings = pd.read_csv(HOLDINGS_FILE)
holdings['asset'] = holdings['asset'].str.upper()
portfolio_assets = set(holdings['asset'])

# Assets that appear in custom_weights (even if you have zero units) are also relevant
custom_weight_assets = set(config.get('custom_weights', {}).keys())

# All candidate assets for dynamic models (exclude USDC for risk-based calculations)
candidate_assets = (portfolio_assets | custom_weight_assets)
candidate_assets.discard('USDC')

# ----------------------------- SELECT MODEL -----------------------------
if MODEL == 'custom':
    target_weights = config.get('custom_weights', {}).copy()
    weight_source = "custom (from config)"
elif MODEL == 'market_cap':
    target_weights = calculate_market_cap_weights()
    weight_source = "market-cap weighted (180-day avg)"
elif MODEL == 'volume_weighted':
    target_weights = calculate_volume_weights()
    weight_source = "volume-weighted (180-day avg)"
elif MODEL == 'risk_parity':
    target_weights = calculate_risk_parity_weights(list(candidate_assets))
    weight_source = "risk-parity / equal risk contribution (180-day returns covariance)"
else:
    raise ValueError(f"Unknown model: {MODEL}")

# Ensure every held asset has a weight (default 0)
for asset in portfolio_assets:
    target_weights.setdefault(asset, 0.0)

# Final weight check
weight_sum = sum(target_weights.values())
if not (0.999999 <= weight_sum <= 1.000001):
    raise ValueError(f"Weights must sum to 1.0 (current sum: {weight_sum:.10f})")
print(f"Model weights OK (sum = {weight_sum:.10f}) [OK]")

# ----------------------------- PRICE FETCHING -----------------------------
def get_price_and_date(ticker: str) -> tuple[float, str]:
    t = ticker.upper()
    if t == 'USDC':
        return 1.0, 'fixed'
    if t not in SYMBOL_FILE_MAP:
        print(f"  Warning: No data file for {t}, using price=0")
        return 0.0, 'no data'
    df = pd.read_csv(SYMBOL_FILE_MAP[t])
    df.columns = df.columns.str.strip()
    df['DATE'] = pd.to_datetime(df['DATE'])
    df = df.sort_values('DATE', ascending=False)

    if PRICING_METHOD == 'latest_close':
        price = df.iloc[0]['PRICE_USD']
        date_used = df.iloc[0]['DATE'].strftime('%Y-%m-%d')
    elif PRICING_METHOD == 'vwap_7d':
        vol_col = None
        for candidate in ['CEX_TRADING_VOLUME_24H_USD', 'TRADING_VOLUME_24H_USD']:
            if candidate in df.columns:
                vol_col = candidate
                break
        if vol_col is None:
            raise ValueError(f"VWAP pricing requires a volume column, but none found for {t}. Available columns: {list(df.columns)}")
        recent = df.head(7)
        price = (recent['PRICE_USD'] * recent[vol_col]).sum() / recent[vol_col].sum()
        date_used = f"7-day VWAP to {df.iloc[0]['DATE'].strftime('%Y-%m-%d')}"
    else:
        raise ValueError("pricing_method must be 'latest_close' or 'vwap_7d'")
    return price, date_used

price_info = {}
for ticker in set(holdings['asset']) | set(target_weights.keys()):
    price, date_str = get_price_and_date(ticker)
    price_info[ticker] = {'price': price, 'date': date_str}

# ----------------------------- PORTFOLIO & REBALANCING -----------------------------
portfolio_rows = []
for _, row in holdings.iterrows():
    asset = row['asset']
    units = row['units']
    price = price_info[asset]['price']
    value = units * price
    portfolio_rows.append({'asset': asset, 'units': units, 'market_price': price, 'value': value})

portfolio_df = pd.DataFrame(portfolio_rows)
total_portfolio_value = portfolio_df['value'].sum() if not portfolio_df.empty else 0
portfolio_df['portfolio_weight'] = portfolio_df['value'] / total_portfolio_value if total_portfolio_value else 0

adjusted_total = total_portfolio_value + CONTRIBUTION

results = []
for _, row in portfolio_df.iterrows():
    asset = row['asset']
    current_value = row['value']
    current_weight = row['portfolio_weight']
    model_weight = target_weights.get(asset, 0.0)
    target_value = model_weight * adjusted_total
    trade_value = target_value - current_value
    trade_units = trade_value / row['market_price'] if row['market_price'] > 0 else 0

    results.append({
        'asset': asset,
        'units': row['units'],
        'market_price': row['market_price'],
        '$ value': current_value,
        'portfolio_weight': current_weight,
        'model_weight': model_weight,
        'difference': current_weight - model_weight,
        'trade_value': trade_value,
        'swap_units': trade_units
    })

results_df = pd.DataFrame(results)

total_trade = results_df['trade_value'].sum()
tolerance = 1e-6 * adjusted_total
if abs(total_trade - CONTRIBUTION) > tolerance:
    raise ValueError(f"Trade sum ({total_trade:,.2f}) != contribution ({CONTRIBUTION:,.2f})")
print("Trade values sum correctly to contribution [OK]")

# ----------------------------- EXCEL OUTPUT -----------------------------
empty_row = {col: '' for col in results_df.columns}
contribution_row = {
    'asset': '', 'units': '', 'market_price': '', '$ value': CONTRIBUTION,
    'portfolio_weight': CONTRIBUTION / total_portfolio_value if total_portfolio_value else 0,
    'model_weight': 0, 'difference': CONTRIBUTION / total_portfolio_value if total_portfolio_value else 0,
    'trade_value': 0, 'swap_units': 0
}
total_row = {
    'asset': 'total', 'units': '', 'market_price': '', '$ value': adjusted_total,
    'portfolio_weight': 1.0, 'model_weight': 1.0, 'difference': 0,
    'trade_value': total_trade, 'swap_units': ''
}

with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    pd.DataFrame([
        ['Portfolio Rebalancing Model'], [''], ['current'],
        ['eligible', 'holdings', '', 'percent', '', '', 'recommended trade'],
        ['assets to use', 'units', 'market price', '$ value', 'portfolio weight', 'model weight', 'difference', 'trade value', 'swap units']
    ]).to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=0)

    results_df.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=6)
    pd.DataFrame([empty_row]).to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=6+len(results_df))
    pd.DataFrame([contribution_row]).to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=6+len(results_df)+2)
    pd.DataFrame([total_row]).to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=6+len(results_df)+4)

    pd.DataFrame([[''], ['Model Choice:'], [''], [weight_source]]).to_excel(
        writer, sheet_name='Sheet1', index=False, header=False, startrow=6+len(results_df)+8)

    price_rows = [['Prices used:']]
    for t, info in price_info.items():
        price_rows.append([f"{t}: ${info['price']:,.2f} ({info['date']})"])
    pd.DataFrame(price_rows).to_excel(writer, sheet_name='Sheet1', index=False, header=False,
                                      startrow=6+len(results_df)+14)

print(f"\nRebalancing complete -> {OUTPUT_FILE}")
print(f"Model used: {weight_source}")
if MODEL == 'risk_parity':
    print("Final risk-parity weights:", {k: f"{v:.4%}" for k, v in target_weights.items() if v > 0})