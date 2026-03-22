import requests
import json
import logging
from typing import Dict, Any

TELEGRAM_TOKEN = "8671494408:AAFPfLQQNWIi2EOhlk1laa-Fn2Gcyj4TrMA"
TELEGRAM_CHAT_ID = "-5247934511"
# try:
#     from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
# except ImportError:
#     TELEGRAM_TOKEN = None
#     TELEGRAM_CHAT_ID = None


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_telegram_message(text: str):

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or TELEGRAM_TOKEN == "your_bot_token_here":
        logger.warning("Telegram token or chat ID not properly configured.")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")
        return False

def format_arbitrage_message(match_details: Dict[str, Any]) -> str:
    kalshi_title = match_details.get('kalshi_market', 'Unknown Kalshi Market')
    poly_title = match_details.get('polymarket_market', 'Unknown Polymarket Market')
    score = match_details.get('combined_score', 0)
    
    direction = match_details.get('direction', 'Unknown Direction')
    profit = match_details.get('expected_profit', 0)
    cost = match_details.get('total_cost', 0)

    # Specific prices based on direction
    if direction == "K_YES_P_NO":
        k_price = match_details.get('k_yes_ask')
        p_price = match_details.get('p_no_ask')
        dir_text = "🟢 Buy Kalshi YES / 🔴 Buy Poly NO"
    else:
        k_price = match_details.get('k_no_ask')
        p_price = match_details.get('p_yes_ask')
        dir_text = "💎 Buy Poly YES / 🏛 Buy Kalshi NO"

    message = f"💰 <b>Arbitrage Opportunity: {profit:.2f}%</b> 💰\n\n"
    message += f"🔄 <b>Strategy:</b> {dir_text}\n"
    message += f"📊 <b>Confidence Score:</b> {score:.2f}\n"
    message += f"💸 <b>Total Cost:</b> {cost:.2f}c\n\n"
    
    message += f"🏛 <b>Kalshi:</b>\n"
    message += f"<code>{kalshi_title}</code>\n"
    message += f"Ask Price: {k_price}\n"
    message += f"Link: <a href='https://kalshi.com/markets/{match_details.get('kalshi_market_ticker')}'>View on Kalshi</a>\n\n"

    message += f"💎 <b>Polymarket:</b>\n"
    message += f"<code>{poly_title}</code>\n"
    message += f"Ask Price: {p_price}\n"

    if match_details.get('polymarket_market_slug'):
        message += f"Link: <a href='https://polymarket.com/event/{match_details.get('polymarket_market_slug')}'>View on Polymarket</a>\n"
    else:
        # Fallback to market ticker if slug not available
        p_ticker = match_details.get('polymarket_market_ticker')
        message += f"ID: {p_ticker}\n"

    message += f"\n<i>Double-check the rules and prices before executing!</i>"
    
    return message

def notify_arbitrage(match_details: Dict[str, Any]):
    """
    Convenience function to format and send a match notification.
    """
    message = format_arbitrage_message(match_details)
    return send_telegram_message(message)

if __name__ == "__main__":

    test_match = {
        "kalshi_market": "Will the Democrats win the NY-15 House seat?",
        "polymarket_market": "New York District 15: Democratic Party wins?",
        "combined_score": 0.95,
        "kalshi_market_ticker": "HOUSE-26-NY-15",
        "polymarket_market_slug": "ny-house-district-15-dem-nominee",
        "kalshi_price": 0.45,
        "polymarket_price": 0.52,
        "arb_impact": 3.0
    }
    notify_arbitrage(test_match)
