# KyberSwap Price Impact Analyzer

## Overview
This project provides a sophisticated tool for analyzing price impacts on KyberSwap's Base network. It continuously monitors and analyzes various token pairs, focusing on their price impacts when swapped against USDC, and stores the results in a PostgreSQL database.

## Features
- Real-time price impact analysis for multiple tokens
- Configurable target price impact ranges
- Smart binary search algorithm for optimal amount discovery
- Continuous monitoring with automatic retries
- PostgreSQL database integration for result storage
- Detailed logging and progress tracking
- Error handling and rate limiting

## Supported Tokens
The analyzer currently supports the following tokens on Base network:
- WETH (Wrapped Ethereum)
- cbETH (Coinbase Wrapped Staked ETH)
- wstETH (Wrapped Staked ETH)
- rETH (Rocket Pool ETH)
- weETH (Wrapped eETH)
- AERO (Aerodrome)
- cbBTC (Coinbase Wrapped BTC)
- EURC (Euro Coin)
- wrsETH (Wrapped rsETH)
- tBTC (Threshold BTC)
- LBTC (Layer2 BTC)
- VIRTUAL (Virtual token)

## Requirements
- Python 3.7+
- PostgreSQL database
- Required Python packages:
  ```
  requests
  psycopg2-binary
  ```

## Installation
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-name>
   ```

2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up your PostgreSQL database and update the connection string in `swapwithsearch.py`:
   ```python
   DATABASE_URL = "your-postgresql-connection-string"
   ```

## Usage

### Basic Usage
Run the script with default parameters:
```bash
python swapwithsearch.py
```

### Advanced Usage
Run with custom parameters:
```bash
python swapwithsearch.py <min_impact> <max_impact> <target_avg> <additional_samples>
```

Example:
```bash
python swapwithsearch.py 4.0 6.0 5.0 5
```

### Parameters
- `min_impact`: Minimum target price impact percentage (default: 4.0)
- `max_impact`: Maximum target price impact percentage (default: 6.0)
- `target_avg`: Target average price impact percentage (default: 5.0)
- `additional_samples`: Number of additional samples to collect (default: 5)

## Database Schema
The script stores results in a PostgreSQL table with the following structure:
- `TimeStamp`: Unix timestamp of the analysis
- Token columns (WETH, cbETH, etc.): Average amounts that achieved the target price impact

## Features in Detail

### Price Impact Analysis
- Uses binary search to find optimal token amounts
- Collects multiple samples around the target impact
- Calculates average amounts and impacts
- Stores results in real-time

### Error Handling
- Automatic retries for failed API calls
- Graceful handling of timeouts
- Continuous operation with error recovery

### Data Collection
- Real-time timestamp recording
- Multiple samples per token
- Statistical analysis of results
- Detailed logging of operations

## Monitoring
The script provides detailed console output including:
- Current analysis progress
- Price impact results
- Statistical summaries
- Error messages and warnings

## Error Messages
Common error messages and their meanings:
- "API returned unsuccessful response": KyberSwap API temporarily unavailable
- "Request failed": Network or connection issue
- "No valid results found": Unable to find amounts within target range
- "Error inserting data into database": Database connection or insertion issue

## Best Practices
1. Monitor the console output for any errors or warnings
2. Adjust target ranges based on market conditions
3. Regular database maintenance
4. Keep API request rates reasonable

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

## License
[Your License Here]

## Disclaimer
This tool is for analysis purposes only. Always perform your own due diligence before making any trading decisions. The tool does not provide financial advice. 