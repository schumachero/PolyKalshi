import json
import os
from datetime import datetime

class EquityTracker:
    def __init__(self, data_file="Data/equity_balances.json"):
        self.data_file = data_file
        self.data = self._load_data()

    def _load_data(self):
        if not os.path.exists(self.data_file):
            return {
                "investors": {}, # format: {"Name": {"shares": 0.0, "total_invested": 0.0, "total_withdrawn": 0.0}}
                "total_shares": 0.0,
                "history": [] # format: list of dicts with timestamp, type, investor, amount, shares, price
            }
        with open(self.data_file, 'r') as f:
            return json.load(f)

    def _save_data(self):
        os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
        with open(self.data_file, 'w') as f:
            json.dump(self.data, f, indent=4)

    def get_price_per_share(self, current_portfolio_value):
        if self.data["total_shares"] == 0:
            return 1.0  # Initial price per share when the fund starts
        return current_portfolio_value / self.data["total_shares"]

    def buy_in(self, investor_name, amount, current_portfolio_value):
        if amount <= 0:
            raise ValueError("Amount must be greater than 0")
        
        price_per_share = self.get_price_per_share(current_portfolio_value)
        shares_bought = amount / price_per_share
        
        if investor_name not in self.data["investors"]:
            self.data["investors"][investor_name] = {"shares": 0.0, "total_invested": 0.0, "total_withdrawn": 0.0}
            
        self.data["investors"][investor_name]["shares"] += shares_bought
        self.data["investors"][investor_name]["total_invested"] += amount
        self.data["total_shares"] += shares_bought
        
        self.data["history"].append({
            "timestamp": datetime.now().isoformat(),
            "type": "BUY",
            "investor": investor_name,
            "amount": amount,
            "shares": shares_bought,
            "price_per_share": price_per_share,
            "portfolio_value_before": current_portfolio_value
        })
        
        self._save_data()
        return shares_bought, price_per_share

    def sell_out(self, investor_name, amount_to_sell, current_portfolio_value):
        if amount_to_sell <= 0:
            raise ValueError("Amount must be greater than 0")
            
        if investor_name not in self.data["investors"]:
            raise ValueError(f"Investor {investor_name} not found")
            
        price_per_share = self.get_price_per_share(current_portfolio_value)
        shares_to_sell = amount_to_sell / price_per_share
        
        current_shares = self.data["investors"][investor_name]["shares"]
        
        # Adding a tiny epsilon to handle minor floating point inaccuracies
        if shares_to_sell > current_shares + 1e-9:
            max_value = current_shares * price_per_share
            raise ValueError(f"Not enough shares. {investor_name} can sell max {max_value:.2f} value.")
        
        # Clamp to max if floating point makes it slightly above
        shares_to_sell = min(shares_to_sell, current_shares)
            
        self.data["investors"][investor_name]["shares"] -= shares_to_sell
        self.data["investors"][investor_name]["total_withdrawn"] += amount_to_sell
        self.data["total_shares"] -= shares_to_sell
        
        self.data["history"].append({
            "timestamp": datetime.now().isoformat(),
            "type": "SELL",
            "investor": investor_name,
            "amount": amount_to_sell,
            "shares": shares_to_sell,
            "price_per_share": price_per_share,
            "portfolio_value_before": current_portfolio_value
        })
        
        self._save_data()
        return shares_to_sell, price_per_share

    def get_status(self, current_portfolio_value):
        status = {}
        total_shares = self.data["total_shares"]
        price_per_share = self.get_price_per_share(current_portfolio_value)
        
        for name, data in self.data["investors"].items():
            value = data["shares"] * price_per_share
            percentage = (data["shares"] / total_shares * 100) if total_shares > 0 else 0
            
            status[name] = {
                "shares": data["shares"],
                "value": value,
                "percentage": percentage,
                "total_invested": data["total_invested"],
                "total_withdrawn": data["total_withdrawn"],
                "profit": value + data["total_withdrawn"] - data["total_invested"]
            }
                
        return {
            "total_portfolio_value": current_portfolio_value,
            "total_shares": total_shares,
            "price_per_share": price_per_share,
            "investor_status": status
        }
