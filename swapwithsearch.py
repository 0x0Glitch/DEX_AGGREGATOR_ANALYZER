import requests
import time
import math
import statistics
import sys
import random
from typing import Dict, Tuple, Optional, Any, List
from datetime import datetime, timezone
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Replace the Supabase setup with direct PostgreSQL connection
DATABASE_URL = "postgresql://postgres.pmmawkmekmzgqoghnxcv:anshuman1@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
TABLE_NAME = "Kyberswap"

def insert_data_to_supabase(row_data: dict):
    """
    Inserts one row (dictionary) into the specified TABLE_NAME using direct PostgreSQL connection.
    The dictionary keys must match the column names in your table.
    """
    try:
        # Create connection
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Fix case-sensitive column names if needed
        fixed_data = {}
        
        # Convert timestamp to numeric format (seconds since epoch)
        if 'timestamp' in row_data:
            if isinstance(row_data['timestamp'], str):
                timestamp_obj = datetime.fromisoformat(row_data['timestamp'].replace('Z', '+00:00'))
                fixed_data['TimeStamp'] = timestamp_obj.timestamp()
            elif isinstance(row_data['timestamp'], (int, float)):
                fixed_data['TimeStamp'] = float(row_data['timestamp'])
            else:
                fixed_data['TimeStamp'] = row_data['timestamp'].timestamp()
        
        # Map token data to correct column names
        token_column_map = {
            'WETH': 'WETH',
            'cbETH': 'cbETH',
            'wstETH': 'wstETH',
            'rETH': 'rETH',
            'weETH': 'weETH',
            'AERO': 'AERO',
            'cbBTC': 'cbBTC',
            'EURC': 'EURC',
            'tBTC': 'tBTC',
            'LBTC': 'LBTC',
            'VIRTUAL': 'VIRTUAL'
        }
        
        # Copy token data with correct column names
        for key, value in row_data.items():
            if key in token_column_map:
                fixed_data[token_column_map[key]] = value
        
        # Check that we have data to insert
        if not fixed_data:
            print("No valid data to insert")
            return
        
        # Prepare the column names and values - quote column names
        columns = ', '.join([f'"{k}"' for k in fixed_data.keys()])
        values = ', '.join(['%s'] * len(fixed_data))
        
        # Create insert query with quoted table name
        query = f'INSERT INTO "{TABLE_NAME}" ({columns}) VALUES ({values})'
        
        # Execute the query with the values
        cur.execute(query, list(fixed_data.values()))
        
        # Commit the transaction
        conn.commit()
        print(f'Inserted data into table "{TABLE_NAME}" successfully')
        print(f'Data inserted: {fixed_data}')
        
    except Exception as e:
        print(f"Error inserting data into database: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

chain = "base"
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

TOKEN_METADATA = {
"0x4200000000000000000000000000000000000006": {  
    "symbol": "WETH", "decimals": 18,
    "default_min": 10, "default_max": 25000
},
"0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22": {  
    "symbol": "cbETH", "decimals": 18,
    "default_min": 100, "default_max": 15000
},
"0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452": {  
    "symbol": "wstETH", "decimals": 18,
    "default_min": 100, "default_max": 10000
},
"0xB6fe221Fe9EeF5aBa221c348bA20A1BF5e73624c": {  
    "symbol": "rETH", "decimals": 18,
    "default_min": 100, "default_max": 15000
},
"0x04C0599Ae5A44757c0af6F9ec3B93da8976c150A": {  
    "symbol": "weETH", "decimals": 18,
    "default_min": 100, "default_max": 15000
},
"0x940181a94A35A4569E4529A3CDfB74e38FD98631": {  
    "symbol": "AERO", "decimals": 18,
    "default_min": 100000, "default_max": 100000000
},
"0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf": {  
    "symbol": "cbBTC", "decimals": 8,
    "default_min": 1, "default_max": 500
},
"0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42": {  
    "symbol": "EURC", "decimals": 6,
    "default_min": 1000000, "default_max": 100000000
},
"0xEdfA23602D0EC14714057867A78D01E94176BEA0": {  
    "symbol": "wrsETH", "decimals": 18,
    "default_min": 100, "default_max": 50000
},
"0x236aa50979D5f3De3Bd1Eeb40E81137F22ab794b": {  
    "symbol": "tBTC", "decimals": 8,
    "default_min": 1, "default_max": 1000
},
"0xECAC9C5F704E954931349DA37F60E39F515C11C1": {  
    "symbol": "LBTC", "decimals": 8,
    "default_min": 1, "default_max": 1000
},
"0x0b3e328455c4059EEb9e3f84b5543F74E24e7E1b": {  
    "symbol": "VIRTUAL", "decimals": 18,
    "default_min": 5000, "default_max": 50000000
}}

def get_token_metadata(token_address: str) -> Dict[str, Any]:
    token_address = token_address.lower()
    for addr, data in TOKEN_METADATA.items():
        if addr.lower() == token_address:
            return data
    return {"symbol": "UNKNOWN", "decimals": 18, "default_min": 1.0, "default_max": 1000.0}

def get_price_impact(token_address: str, amount_in: int, retries: int = 3) -> Tuple[Optional[float], Optional[float]]:
    url = (
        f"https://aggregator-api.kyberswap.com/{chain}/api/v1/routes"
        f"?tokenIn={token_address}"
        f"&tokenOut={USDC_ADDRESS}"
        f"&amountIn={amount_in}"
        f"&gasInclude=true"
    )
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get("code") == 0 and data.get("data"):
                route = data["data"]["routeSummary"]
                price_impact = ((float(route['amountInUsd']) - float(route['amountOutUsd'])) / 
                                float(route['amountInUsd'])) * 100
                output_amount = int(route['amountOut']) / 10**6
                return abs(price_impact), output_amount
            
            if attempt < retries - 1:
                print(f"API returned unsuccessful response, retrying... ({attempt+1}/{retries})")
                time.sleep(1)
            else:
                print(f"API returned unsuccessful response after {retries} attempts")
                return None, None
                
        except Exception as e:
            if attempt < retries - 1:
                print(f"Request failed, retrying... ({attempt+1}/{retries}): {str(e)}")
                time.sleep(1)
            else:
                print(f"Request failed after {retries} attempts: {str(e)}")
                return None, None
    
    return None, None

def generate_refined_amounts(base_amount: float, variation_pct: float = 5.0, count: int = 5) -> List[float]:
    refined_amounts = []
    variations = []
    
    for i in range(count // 2 + count % 2):
        factor = (i + 1) / (count // 2 + 1)
        variations.append(-variation_pct * factor)
    
    for i in range(count // 2):
        factor = (i + 1) / (count // 2 + 1)
        variations.append(variation_pct * factor)
    
    variations.sort()
    
    for var in variations:
        refined_amounts.append(base_amount * (1 + var/100))
    
    return refined_amounts

def find_amount_for_exact_impact(token_address: str, 
                                 target_min: float, 
                                 target_max: float,
                                 max_iterations: int = 25,
                                 timeout: int = 180) -> Optional[Dict]:
    start_time = time.time()
    
    metadata = get_token_metadata(token_address)
    symbol = metadata["symbol"]
    decimals = metadata["decimals"]
    min_amount = metadata["default_min"]
    max_amount = metadata["default_max"]
    
    print(f"Searching for {symbol} amount that produces {target_min}-{target_max}% price impact...")
    
    iterations = 0
    low = min_amount
    high = max_amount
    best_amount = None
    best_impact = None
    best_output = None
    
    print("\n--- Phase 1: Quick Range Finding ---")
    current = min_amount
    growth_factor = 2.0
    found_high_impact = False
    
    while current <= max_amount and not found_high_impact and (time.time() - start_time) < timeout/3:
        iterations += 1
        print(f"\nIteration {iterations}/{max_iterations}: Trying {current:.4f} {symbol}...")
        
        amount_smallest = int(current * 10**decimals)
        impact, output = get_price_impact(token_address, amount_smallest)
        
        if impact is None:
            print("API call failed, retrying...")
            time.sleep(1)
            continue
            
        print(f"→ Price impact: {impact:.4f}%, Output: {output:.2f} USDC")
        
        target_mid = (target_min + target_max)/2
        if best_impact is None or abs(impact - target_mid) < abs(best_impact - target_mid):
            best_amount = current
            best_impact = impact
            best_output = output
            print(f"→ New best match so far!")
        
        if impact >= target_min:
            found_high_impact = True
            print(f"→ Found impact at or above minimum target {target_min}%!")
            high = current
            if impact > target_max:
                low = current / growth_factor
                print(f"→ Impact above maximum target, setting range to {low:.4f} - {high:.4f}")
            else:
                print(f"✓ Found amount within target range: {current:.4f} {symbol} → {impact:.4f}%")
                return {
                    "amount": current,
                    "impact": impact,
                    "output": output
                }
            break
        
        low = current
        current *= growth_factor
        print(f"→ Impact too low, increasing to {current:.4f}")
        
        time.sleep(0.1)
        
    if not found_high_impact:
        if best_impact is not None and best_impact > 0:
            print(f"\nReached maximum amount ({max_amount} {symbol}) with impact {best_impact:.4f}%")
            if best_impact > target_min * 0.7:
                print(f"Impact is close enough to consider. Returning best result.")
                return {
                    "amount": best_amount,
                    "impact": best_impact,
                    "output": best_output
                }
            else:
                print(f"Impact too far below target. No valid result found.")
                return None
        else:
            print(f"\nFailed to find any valid impact results up to {max_amount} {symbol}")
            return None
    
    print("\n--- Phase 2: Binary Search Refinement ---")
    binary_iterations = 0
    max_binary_iterations = 15
    
    while binary_iterations < max_binary_iterations and high > low * 1.01 and (time.time() - start_time) < timeout:
        iterations += 1
        binary_iterations += 1
        
        mid = (low + high) / 2
        print(f"\nRefinement {binary_iterations}/{max_binary_iterations}: Trying {mid:.4f} {symbol}...")
        
        amount_smallest = int(mid * 10**decimals)
        impact, output = get_price_impact(token_address, amount_smallest)
        
        if impact is None:
            print("API call failed, retrying...")
            time.sleep(1)
            continue
            
        print(f"→ Price impact: {impact:.4f}%, Output: {output:.2f} USDC")
        
        target_mid = (target_min + target_max)/2
        if abs(impact - target_mid) < abs(best_impact - target_mid):
            best_amount = mid
            best_impact = impact
            best_output = output
            print(f"→ New best match so far!")
        
        if target_min <= impact <= target_max:
            print(f"✓ Found amount within target range!")
            return {
                "amount": mid,
                "impact": impact,
                "output": output
            }
            
        if impact < target_min:
            low = mid
            print(f"→ Impact too low, increasing lower bound to {low:.4f}")
        else:
            high = mid
            print(f"→ Impact too high, decreasing upper bound to {high:.4f}")
            
        time.sleep(0.1)
    
    print(f"\nBinary search complete after {binary_iterations} refinement iterations.")
    
    if best_impact is not None:
        print(f"Best result: {best_amount:.6f} {symbol} → {best_impact:.4f}% impact → {best_output:.2f} USDC")
        
        if abs(best_impact - (target_min + target_max)/2) < (target_max - target_min):
            print(f"Impact close to target range. Returning best result.")
            return {
                "amount": best_amount,
                "impact": best_impact,
                "output": best_output
            }
        else:
            print(f"Impact too far from target range. No valid result found.")
            return None
    else:
        print(f"Failed to find any valid results.")
        return None

def collect_impact_results(token_address: str, base_result: Dict, 
                           target_min: float, target_max: float,
                           target_avg: float = 5.0,
                           additional_samples: int = 5) -> List[Dict]:
    metadata = get_token_metadata(token_address)
    symbol = metadata["symbol"]
    decimals = metadata["decimals"]
    base_amount = base_result["amount"]
    base_impact = base_result["impact"]
    
    print(f"\nCollecting {additional_samples} additional samples targeting average impact of {target_avg:.2f}%...")
    
    results = [base_result]
    
    impact_diff = target_avg - base_impact
    
    target_others_avg = (target_avg * (additional_samples + 1) - base_impact) / additional_samples
    target_others_avg = max(target_min, min(target_max, target_others_avg))
    
    print(f"Base impact: {base_impact:.4f}%")
    print(f"To achieve {target_avg:.4f}% average, other samples should average {target_others_avg:.4f}%")
    
    if additional_samples >= 3:
        if target_others_avg < target_avg:
            relative_targets = [-0.8, -0.3, 0, 0.4, 0.8]
        else:
            relative_targets = [-0.8, -0.4, 0, 0.3, 0.8]
    else:
        relative_targets = []
        for i in range(additional_samples):
            relative_targets.append(-0.8 + 1.6 * i / (additional_samples - 1))
    
    relative_targets = relative_targets[:additional_samples]
    
    usable_range = min(target_max - target_min, 1.5)
    
    impact_targets = []
    for rel in relative_targets:
        impact_val = target_others_avg + rel * usable_range / 2
        impact_val = max(target_min, min(target_max, impact_val))
        impact_targets.append(impact_val)
    
    print(f"Target impacts: {[f'{imp:.4f}%' for imp in impact_targets]}")
    
    for i, target_impact in enumerate(impact_targets, 1):
        print(f"\nFinding amount for target impact {i}/{len(impact_targets)}: {target_impact:.4f}%...")
        
        impact_ratio = target_impact / base_impact if base_impact > 0 else 1.0
        test_amount = base_amount * impact_ratio
        
        max_attempts = 3
        low = base_amount * 0.8
        high = base_amount * 1.5
        
        if base_impact < target_min:
            low = base_amount
            high = base_amount * 2.0
        elif base_impact > target_max:
            low = base_amount * 0.5
            high = base_amount
        
        found_valid = False
        
        for attempt in range(max_attempts):
            print(f"  Attempt {attempt+1}/{max_attempts}: Trying {test_amount:.6f} {symbol}...")
            
            amount_smallest = int(test_amount * 10**decimals)
            impact, output = get_price_impact(token_address, amount_smallest)
            
            if impact is None:
                print("  API call failed, skipping...")
                break
                
            print(f"  → Price impact: {impact:.4f}%, Output: {output:.2f} USDC")
            
            if target_min <= impact <= target_max:
                print(f"  ✓ Found amount with impact in target range!")
                results.append({
                    "amount": test_amount,
                    "impact": impact,
                    "output": output
                })
                found_valid = True
                
                if abs(impact - target_impact) < 0.2:
                    print(f"  ✓ Impact is close to target!")
                    break
            
            if impact < target_impact:
                low = test_amount
                test_amount = (test_amount + high) / 2
                print(f"  → Impact too low, increasing to {test_amount:.6f}")
            else:
                high = test_amount
                test_amount = (test_amount + low) / 2
                print(f"  → Impact too high, decreasing to {test_amount:.6f}")
                
            if (high - low) / low < 0.02:
                print(f"  → Adjustment too small, stopping search.")
                break
        
        if not found_valid and attempt == max_attempts - 1:
            adjusted_target = (target_impact + target_avg) / 2
            print(f"\n  No valid result found. Trying adjusted target: {adjusted_target:.4f}%...")
            
            adjusted_smallest = int(test_amount * 10**decimals)
            impact, output = get_price_impact(token_address, adjusted_smallest)
            
            if impact is not None and target_min <= impact <= target_max:
                print(f"  ✓ Found amount with impact in target range!")
                results.append({
                    "amount": test_amount,
                    "impact": impact,
                    "output": output
                })
                found_valid = True
    
    valid_results = [r for r in results if target_min <= r["impact"] <= target_max]
    
    print(f"\nCollected {len(valid_results)} valid results within target range {target_min}%-{target_max}%.")
    
    if len(valid_results) > 0:
        current_avg = sum(r["impact"] for r in valid_results) / len(valid_results)
        print(f"Current average impact: {current_avg:.4f}% (target: {target_avg:.4f}%)")
    
    if len(valid_results) < additional_samples + 1:
        missing = additional_samples + 1 - len(valid_results)
        print(f"Missing {missing} samples. Making final attempts to fill in...")
        
        if valid_results:
            valid_results.sort(key=lambda x: x["impact"])
            
            for _ in range(missing):
                current_avg = sum(r["impact"] for r in valid_results) / len(valid_results)
                needed_impact = (target_avg * (len(valid_results) + 1) - 
                                 sum(r["impact"] for r in valid_results))
                
                needed_impact = max(target_min, min(target_max, needed_impact))
                
                print(f"\nNeed impact of {needed_impact:.4f}% to reach target average...")
                
                closest_idx = 0
                closest_diff = abs(valid_results[0]["impact"] - needed_impact)
                
                for i, r in enumerate(valid_results):
                    diff = abs(r["impact"] - needed_impact)
                    if diff < closest_diff:
                        closest_diff = diff
                        closest_idx = i
                
                ref_result = valid_results[closest_idx]
                impact_ratio = needed_impact / ref_result["impact"]
                test_amount = ref_result["amount"] * impact_ratio
                
                print(f"Trying amount: {test_amount:.6f} {symbol}...")
                
                amount_smallest = int(test_amount * 10**decimals)
                impact, output = get_price_impact(token_address, amount_smallest)
                
                if impact is None:
                    print("API call failed, skipping...")
                    continue
                    
                print(f"→ Price impact: {impact:.4f}%, Output: {output:.2f} USDC")
                
                if target_min <= impact <= target_max:
                    print(f"✓ Amount within target range!")
                    valid_results.append({
                        "amount": test_amount,
                        "impact": impact,
                        "output": output
                    })
                else:
                    print(f"❌ Amount outside target range.")
    
    if len(valid_results) > 0:
        final_avg = sum(r["impact"] for r in valid_results) / len(valid_results)
        print(f"Final average impact: {final_avg:.4f}% (target: {target_avg:.4f}%)")
        print(f"Deviation from target: {abs(final_avg - target_avg):.4f}%")
    
    return valid_results

def calculate_stats(results):
    if not results:
        return {
            "avg_amount": None,
            "avg_impact": None,
            "avg_output": None,
            "min_amount": None,
            "max_amount": None,
            "count": 0
        }
    
    amounts = [r["amount"] for r in results]
    impacts = [r["impact"] for r in results]
    outputs = [r["output"] for r in results]
    
    stats = {
        "avg_amount": statistics.mean(amounts) if amounts else None,
        "avg_impact": statistics.mean(impacts) if impacts else None,
        "avg_output": statistics.mean(outputs) if outputs else None,
        "min_amount": min(amounts) if amounts else None,
        "max_amount": max(amounts) if amounts else None,
        "count": len(results)
    }
    
    return stats

def analyze_token(token_address: str, target_min: float, target_max: float, 
                  target_avg: float = 5.0, additional_samples: int = 5):
    metadata = get_token_metadata(token_address)
    symbol = metadata["symbol"]
    
    try:
        base_result = find_amount_for_exact_impact(
            token_address=token_address,
            target_min=target_min,
            target_max=target_max
        )
        
        if not base_result:
            print(f"❌ Could not find a {symbol} amount with impact between {target_min}%-{target_max}%")
            return None
        
        results = collect_impact_results(
            token_address=token_address,
            base_result=base_result,
            target_min=target_min,
            target_max=target_max,
            target_avg=target_avg,
            additional_samples=additional_samples
        )
        
        stats = calculate_stats(results)
        
        return {
            "symbol": symbol,
            "address": token_address,
            "results": results,
            "stats": stats
        }
        
    except KeyboardInterrupt:
        print(f"\n⚠️ Analysis interrupted for {symbol}")
        return None
    except Exception as e:
        print(f"\n❌ Error analyzing {symbol}: {str(e)}")
        return None

def run_token_analysis(token_list,
                       target_min=4,
                       target_max=6,
                       target_avg=5.0,
                       additional_samples=5):
    successful_matches = []
    failed_tokens = []
    
    # Get current UTC timestamp without modifying to midnight
    current_time = datetime.now(timezone.utc)
    unix_timestamp = current_time.timestamp()  # Current Unix timestamp
    
    print(f"\nStarting analysis at Unix timestamp: {unix_timestamp}")
    print(f"Corresponding to: {current_time} UTC")
    
    for token_address in token_list:
        try:
            metadata = get_token_metadata(token_address)
            symbol = metadata["symbol"]
            
            print(f"\n{'='*50}")
            print(f"ANALYZING {symbol}")
            print(f"{'='*50}")
            
            # Analyze token without asking for confirmation
            result = analyze_token(
                token_address=token_address,
                target_min=target_min,
                target_max=target_max,
                target_avg=target_avg,
                additional_samples=additional_samples
            )
            
            if result and result["stats"]["count"] > 0:
                successful_matches.append(result)
                
                print(f"\n{'+'*40}")
                print(f"RESULTS FOR {symbol}")
                print(f"{'+'*40}")
                print(f"Found {result['stats']['count']} amounts with price impact {target_min}%-{target_max}%")
                print(f"Average amount: {result['stats']['avg_amount']:.6f} {symbol}")
                print(f"Average impact: {result['stats']['avg_impact']:.4f}%")
                print(f"Average output: {result['stats']['avg_output']:.2f} USDC")
                print(f"Deviation from target: {abs(result['stats']['avg_impact'] - target_avg):.4f}%")
            else:
                failed_tokens.append({"symbol": symbol, "reason": "No valid results found"})
        
        except KeyboardInterrupt:
            print(f"\n⚠️ Analysis interrupted for {symbol}. Moving to next token...")
            failed_tokens.append({"symbol": symbol, "reason": "Interrupted"})
        except Exception as e:
            print(f"\n❌ Error processing {token_address}: {str(e)}")
            failed_tokens.append({"symbol": symbol, "reason": f"Error: {str(e)}"})
        
        print("\n" + "-" * 50)
    
    print("\n" + "=" * 100)
    print(f"SUMMARY OF TOKENS MATCHING TARGET SLIPPAGE RANGE ({target_min}% - {target_max}%)")
    print("=" * 100)
    
    # We gather a single dictionary row for this entire iteration:
    row_data = {"timestamp": unix_timestamp}  # Use the Unix timestamp for midnight UTC

    # Initialize every token column to None
    token_symbols = [data["symbol"] for data in TOKEN_METADATA.values()]
    for sym in token_symbols:
        row_data[sym] = None

    if successful_matches:
        print(f"{'Token':<10} {'Avg Amount':<15} {'Avg Impact %':<12} {'Avg Output USDC':<15} {'# Samples':<10} {'Dev from {target_avg}%':<18}")
        print("-" * 80)
        for match in successful_matches:
            stats = match["stats"]
            symbol = match["symbol"]
            if stats["count"] > 0:
                deviation = abs(stats["avg_impact"] - target_min)  # or from target_avg if you prefer
                print(f"{symbol:<10} {stats['avg_amount']:<15.4f} {stats['avg_impact']:<12.2f} {stats['avg_output']:<15.2f} {stats['count']:<10} {deviation:<18.4f}")
                
                # Store the average amount for this token in row_data
                row_data[symbol] = stats["avg_amount"]
        
        print("\n" + "=" * 100)
        print("DETAILED RESULTS FOR EACH TOKEN")
        print("=" * 100)
        
        for match in successful_matches:
            symbol = match["symbol"]
            results = match["results"]
            stats = match["stats"]
            
            print(f"\n{symbol} - Found {stats['count']} samples in target range")
            print(f"{'Amount':<15} {'Impact %':<10} {'Output USDC':<15}")
            print("-" * 45)
            
            sorted_results = sorted(results, key=lambda x: x["impact"])
            
            for r in sorted_results:
                print(f"{r['amount']:<15.6f} {r['impact']:<10.4f} {r['output']:<15.2f}")
            
            print(f"\nAverage: {stats['avg_amount']:.6f} {stats['avg_impact']:.4f} {stats['avg_output']:.2f}")
            print(f"Deviation from target: {abs(stats['avg_impact'] - target_max):.4f}%")
    else:
        print("No tokens found within the target slippage range.")
    
    if failed_tokens:
        print("\nTokens with no valid results:")
        for token in failed_tokens:
            print(f" - {token['symbol']}: {token['reason']}")

    # ------------------ Insert row_data into Supabase ------------------
    insert_data_to_supabase(row_data)
    # -------------------------------------------------------------------
    
    return successful_matches

def run_continuous_analysis():
    """Run the analysis in a continuous loop with a pause between runs"""
    
    target_min = 4.0
    target_max = 6.0
    target_avg = 5.0
    additional_samples = 5
    
    # Use command line args if provided (optional)
    if len(sys.argv) > 2:
        try:
            target_min = float(sys.argv[1])
            target_max = float(sys.argv[2])
            if len(sys.argv) > 3:
                target_avg = float(sys.argv[3])
            if len(sys.argv) > 4:
                additional_samples = int(sys.argv[4])
        except ValueError:
            pass
    
    # Always analyze all tokens
    all_tokens = list(TOKEN_METADATA.keys())
    
    while True:
        try:
            print("\n" + "=" * 80)
            print(f"PRICE IMPACT FINDER v2.5 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Finding token amounts with targeted average price impact of {target_avg}%")
            print(f"Target range: {target_min}%-{target_max}%")
            print("=" * 80)
            
            print("\nAnalyzing all tokens automatically...")
            
            # Run the analysis
            run_token_analysis(
                all_tokens, 
                target_min=target_min, 
                target_max=target_max, 
                target_avg=target_avg,
                additional_samples=additional_samples
            )
            
            print("\n" + "=" * 80)
            print(f"Analysis complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("Waiting 20 seconds before restarting...")
            print("=" * 80)
            
            # Wait before restarting
            time.sleep(1)
            
        except KeyboardInterrupt:
            print("\n\nScript interrupted by user. Exiting loop...")
            break
        except Exception as e:
            print(f"\nError in main loop: {str(e)}")
            print("Restarting in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    try:
        # Start continuous analysis loop
        run_continuous_analysis()
    except KeyboardInterrupt:
        print("\n\nScript terminated by user. Exiting...")
    except Exception as e:
        print(f"\nFatal error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("\nScript ended.")
