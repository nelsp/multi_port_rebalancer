# Portfolio Rebalancing Tool

A Python tool for calculating portfolio rebalancing recommendations based on various weighting models.

## Features

- **Multiple Weighting Models**:
  - Custom weights (manual specification)
  - Market-cap weighted (based on circulating supply × price)
  - Volume-weighted (based on 24h trading volume)
  - Risk-parity (equal risk contribution optimization)

- **Pricing Methods**:
  - Latest close price
  - 7-day VWAP (Volume Weighted Average Price)

- **Output**: Excel file with detailed rebalancing recommendations

## Requirements

- Python 3.11+
- pandas
- numpy
- scipy
- pyyaml
- openpyxl

## Installation

```bash
pip install pandas numpy scipy pyyaml openpyxl
```

## Configuration

Edit `config.yaml` to set:
- `pricing_method`: `latest_close` or `vwap_7d`
- `contribution`: Dollar amount to add/remove from portfolio
- `model`: `custom`, `market_cap`, `volume_weighted`, or `risk_parity`
- `custom_weights`: Asset weights (only used for custom model)

## Data Files

Place your historical data CSV files in the `Data/` directory with the naming pattern:
`{TICKER}_{DATE}_180days.csv`

Required columns:
- `DATE`
- `PRICE_USD`
- `CIRCULATING_TOKENS` (for market-cap model)
- `CEX_TRADING_VOLUME_24H_USD` (for volume-weighted model and VWAP pricing)

## Holdings File

Create a `holdings.csv` file with columns:
- `asset`: Asset ticker symbol
- `units`: Current number of units held

## Usage

```bash
python rebalance.py
```

The script will generate an Excel file named `port_rebal_{DATE}.xlsx` with rebalancing recommendations.

## Output

The Excel file contains:
- Current portfolio holdings and values
- Target weights based on selected model
- Recommended trades (buy/sell amounts)
- Price information used for calculations

