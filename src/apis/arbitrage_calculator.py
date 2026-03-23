"""
Arbitrage Calculator

Finds the maximum volume that can be bought across two platform orderbooks 
before the combined marginal price (plus fees) exceeds a target threshold.
"""
import copy

# =========================
# Configuration
# =========================

# Kalshi taker fee coefficient: fee = KALSHI_FEE_COEFFICIENT * P * (1 - P)
KALSHI_FEE_COEFFICIENT = 0.07

# Default Polymarket fee rate (0% for standard markets)
DEFAULT_POLY_FEE_RATE = 0.0

# Default price threshold for the volume-filling arbitrage scanner
DEFAULT_PRICE_THRESHOLD = 1.10

# Default threshold for the quick-check single-level arbitrage test
QUICK_CHECK_THRESHOLD = 0.95

# =========================
# Fee Calculations
# =========================

def calculate_kalshi_marginal_fee(price):
    """
    Kalshi charges taker fees on matched contracts.
    Formula per contract: KALSHI_FEE_COEFFICIENT * P * (1-P)
    """
    return KALSHI_FEE_COEFFICIENT * price * (1.0 - price)

def calculate_poly_marginal_fee(price, fee_rate=DEFAULT_POLY_FEE_RATE):
    """
    Polymarket standard markets are 0%. 
    Can be configured for 15-min crypto markets if needed via fee_rate.
    """
    return price * fee_rate

def find_arbitrage_volume(kalshi_asks, poly_asks, price_threshold=DEFAULT_PRICE_THRESHOLD, poly_fee_rate=DEFAULT_POLY_FEE_RATE):
    """
    Merges two orderbooks slice by slice, buying the cheapest available paired contracts.
    Stops when the combined price of the pair (including fees) exceeds the price_threshold.
    
    Returns:
        total_volume, total_cost, slices_taken
    """
    # Deep copy to avoid mutating the original fetched orderbooks
    k_asks = copy.deepcopy(kalshi_asks)
    p_asks = copy.deepcopy(poly_asks)
    
    k_idx = 0
    p_idx = 0
    
    total_volume = 0.0
    total_cost = 0.0
    total_k_cost = 0.0
    total_p_cost = 0.0
    total_k_fee = 0.0
    total_p_fee = 0.0
    
    slices = []
    
    while k_idx < len(k_asks) and p_idx < len(p_asks):
        k_ask = k_asks[k_idx]
        p_ask = p_asks[p_idx]
        
        # Calculate marginal combination price per 1 contract (including fees)
        k_price = k_ask["price"]
        p_price = p_ask["price"]
        
        k_fee = calculate_kalshi_marginal_fee(k_price)
        p_fee = calculate_poly_marginal_fee(p_price, poly_fee_rate)
        
        combined_marginal_price = k_price + k_fee + p_price + p_fee
        
        # Check against threshold
        if combined_marginal_price > price_threshold:
            break
            
        # Determine maximum volume we can clear at this exact price combination
        available_vol = min(k_ask["volume"], p_ask["volume"])
        
        if available_vol <= 0:
            if k_ask["volume"] <= 0: k_idx += 1
            if p_ask["volume"] <= 0: p_idx += 1
            continue
            
        # Execute the slice
        slice_k_cost = available_vol * k_price
        slice_p_cost = available_vol * p_price
        slice_k_fee = available_vol * k_fee
        slice_p_fee = available_vol * p_fee
        slice_total_cost = slice_k_cost + slice_p_cost + slice_k_fee + slice_p_fee
        
        total_volume += available_vol
        total_k_cost += slice_k_cost
        total_p_cost += slice_p_cost
        total_k_fee += slice_k_fee
        total_p_fee += slice_p_fee
        total_cost += slice_total_cost
        
        slices.append({
            "combined_marginal_price": round(combined_marginal_price, 4),
            "volume_cleared": available_vol,
            "kalshi_price": k_price,
            "kalshi_fee": round(k_fee, 4),
            "poly_price": p_price,
            "poly_fee": round(p_fee, 4)
        })
        
        # Deduct volume from the resting books
        k_asks[k_idx]["volume"] -= available_vol
        p_asks[p_idx]["volume"] -= available_vol
        
        # Advance index if a level is fully consumed
        if k_asks[k_idx]["volume"] <= 1e-8:
            k_idx += 1
        if p_asks[p_idx]["volume"] <= 1e-8:
            p_idx += 1

    return {
        "executable_volume": total_volume,
        "total_cost": round(total_cost, 4),
        "total_base_cost": round(total_k_cost + total_p_cost, 4),
        "total_fees": round(total_k_fee + total_p_fee, 4),
        "vwap_price": round(total_cost / total_volume, 4) if total_volume > 0 else 0.0,
        "kalshi_leg_cost": round(total_k_cost, 4),
        "kalshi_leg_fees": round(total_k_fee, 4),
        "polymarket_leg_cost": round(total_p_cost, 4),
        "polymarket_leg_fees": round(total_p_fee, 4),
        "execution_slices": slices
    }


def quick_check_arbitrage(orderbooks, threshold=QUICK_CHECK_THRESHOLD, poly_fee_rate=DEFAULT_POLY_FEE_RATE):
    """
    Quickly checks if the best available prices on the first level of the orderbooks 
    (including fees) offer an arbitrage opportunity below the given threshold.
    Returns True if an opportunity exists, False otherwise.
    """
    if not orderbooks:
        return False
        
    kalshi = orderbooks.get("kalshi", {})
    poly = orderbooks.get("polymarket", {})
    
    # Strategy 1: Buy YES on Kalshi AND NO on Polymarket
    k_yes_asks = kalshi.get("yes", {}).get("asks", [])
    p_no_asks = poly.get("no", {}).get("asks", [])
    
    if k_yes_asks and p_no_asks:
        k_price = k_yes_asks[0]["price"]
        p_price = p_no_asks[0]["price"]
        k_fee = calculate_kalshi_marginal_fee(k_price)
        p_fee = calculate_poly_marginal_fee(p_price, poly_fee_rate)
        if (k_price + k_fee + p_price + p_fee) < threshold:
            return True

    # Strategy 2: Buy NO on Kalshi AND YES on Polymarket
    k_no_asks = kalshi.get("no", {}).get("asks", [])
    p_yes_asks = poly.get("yes", {}).get("asks", [])
    
    if k_no_asks and p_yes_asks:
        k_price = k_no_asks[0]["price"]
        p_price = p_yes_asks[0]["price"]
        k_fee = calculate_kalshi_marginal_fee(k_price)
        p_fee = calculate_poly_marginal_fee(p_price, poly_fee_rate)
        if (k_price + k_fee + p_price + p_fee) < threshold:
            return True
            
    return False

def get_best_combo_price(orderbooks, poly_fee_rate=DEFAULT_POLY_FEE_RATE):
    """
    Checks the first level of the orderbooks and returns the lowest combined 
    marginal price for buying 1 unit of a paired market.
    """
    if not orderbooks:
        return None
        
    kalshi = orderbooks.get("kalshi", {})
    poly = orderbooks.get("polymarket", {})
    
    best_price = float('inf')
    best_strategy = None
    
    # Strategy 1: Buy YES on Kalshi AND NO on Polymarket
    k_yes_asks = kalshi.get("yes", {}).get("asks", [])
    p_no_asks = poly.get("no", {}).get("asks", [])
    
    if k_yes_asks and p_no_asks:
        k_price = k_yes_asks[0]["price"]
        p_price = p_no_asks[0]["price"]
        k_fee = calculate_kalshi_marginal_fee(k_price)
        p_fee = calculate_poly_marginal_fee(p_price, poly_fee_rate)
        total = k_price + k_fee + p_price + p_fee
        if total < best_price:
            best_price = total
            best_strategy = "Buy Kalshi YES / Poly NO"

    # Strategy 2: Buy NO on Kalshi AND YES on Polymarket
    k_no_asks = kalshi.get("no", {}).get("asks", [])
    p_yes_asks = poly.get("yes", {}).get("asks", [])
    
    if k_no_asks and p_yes_asks:
        k_price = k_no_asks[0]["price"]
        p_price = p_yes_asks[0]["price"]
        k_fee = calculate_kalshi_marginal_fee(k_price)
        p_fee = calculate_poly_marginal_fee(p_price, poly_fee_rate)
        total = k_price + k_fee + p_price + p_fee
        if total < best_price:
            best_price = total
            best_strategy = "Buy Kalshi NO / Poly YES"
            
    if best_price == float('inf'):
        return None
        
    return {
        "price": round(best_price, 4),
        "strategy": best_strategy
    }

def calculate_arbitrage(orderbooks, price_threshold=DEFAULT_PRICE_THRESHOLD, poly_fee_rate=DEFAULT_POLY_FEE_RATE):
    """
    Evaluates both directional strategies to find executable volume below the price threshold.
    """
    results = {}
    
    if not orderbooks:
        return results
        
    kalshi = orderbooks.get("kalshi", {})
    poly = orderbooks.get("polymarket", {})
    
    # Strategy 1: Buy YES on Kalshi AND NO on Polymarket
    k_yes_asks = kalshi.get("yes", {}).get("asks", [])
    p_no_asks = poly.get("no", {}).get("asks", [])
    results["buy_yes_kalshi_no_poly"] = find_arbitrage_volume(
        k_yes_asks, p_no_asks, price_threshold, poly_fee_rate
    )
    
    # Strategy 2: Buy NO on Kalshi AND YES on Polymarket
    k_no_asks = kalshi.get("no", {}).get("asks", [])
    p_yes_asks = poly.get("yes", {}).get("asks", [])
    results["buy_no_kalshi_yes_poly"] = find_arbitrage_volume(
        k_no_asks, p_yes_asks, price_threshold, poly_fee_rate
    )

    return results
