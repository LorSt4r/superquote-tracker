import asyncio
import hashlib
import json
import logging
import os
import random
import re
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp
import gspread
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

# --- Logging Configuration ---
# Sets up a professional logging format with timestamps and severity levels
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler()  # Logs to console. Can add FileHandler here to save logs to a file.
    ]
)
logger = logging.getLogger(__name__)

# --- Constants & Mappings ---
SPORT_ICON_MAP = {
    '1': 'Soccer', '2': 'Horse Racing', '3': 'Cricket', '5': 'Specials', '7': 'Golf',
    '8': 'Rugby Union', '9': 'Boxing', '10': 'Formula 1', '12': 'Tennis',
    '14': 'Snooker', '15': 'Darts', '16': 'Baseball', '17': 'Ice Hockey',
    '18': 'Basketball', '19': 'Rugby League', '24': 'Speedway', '36': 'Aussie Rules',
    '38': 'Cycling', '78': 'Handball', '83': 'Futsal'
}

class SuperquoteBot:
    """
    A robust automation bot to track and analyze 'Superquotes' (Value Bets) on Bet365.
    Handles scraping, notifications, data logging, and error recovery.
    """

    def __init__(self):
        # Load environment variables
        load_dotenv()
        self._validate_config()

        # State management
        self.active_superquotes: Dict[str, dict] = {}
        self.history: Dict[str, dict] = self._load_history()
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.gs_client: Optional[gspread.Client] = None
        self.spreadsheet = None

        # Initialize Google Sheets connection if enabled
        if self.enable_gsheets:
            self._init_google_sheets()

    def _validate_config(self):
        """Validates that all required environment variables are set."""
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_ids = os.getenv("TELEGRAM_CHAT_IDS", "").split(",")
        self.history_file = os.getenv("SUPERQUOTE_HISTORY_FILE", "superquote_history.json")
        self.healthcheck_url = os.getenv("HEALTHCHECK_URL") # Moved to .env for security

        # Google Sheets Config
        self.gs_creds_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
        self.gs_sheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
        self.gs_worksheet_name = os.getenv("GOOGLE_SHEETS_WORKSHEET_NAME", "Database")
        
        # Determine if Sheets should be enabled
        self.enable_gsheets = all([self.gs_creds_file, self.gs_sheet_id, os.path.exists(str(self.gs_creds_file))])

        if not self.telegram_token or not self.telegram_chat_ids[0]:
            logger.critical("Missing required Telegram configuration in .env")
            raise ValueError("Invalid Configuration")

    def _init_google_sheets(self):
        """Authenticates with Google Sheets API."""
        try:
            scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file']
            creds = Credentials.from_service_account_file(self.gs_creds_file, scopes=scopes)
            self.gs_client = gspread.authorize(creds)
            self.spreadsheet = self.gs_client.open_by_key(self.gs_sheet_id)
            logger.info(f"Connected to Google Spreadsheet: {self.spreadsheet.title}")
        except Exception as e:
            logger.error(f"Failed to connect to Google Sheets: {e}")
            self.enable_gsheets = False

    def _load_history(self) -> dict:
        """Loads historical data from JSON file."""
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Filter only currently active bets from history
                    self.active_superquotes = {k: v for k, v in data.items() if v.get('active') is True}
                    logger.info(f"Loaded {len(data)} historical records ({len(self.active_superquotes)} active).")
                    return data
            except json.JSONDecodeError:
                logger.warning("History file is corrupted. Starting fresh.")
        return {}

    def _save_history(self):
        """Persists historical data to JSON file."""
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(self.history, f, indent=2, ensure_ascii=False)
        except IOError as e:
            logger.error(f"Failed to save history: {e}")

    def _generate_id(self, info: dict) -> str:
        """Generates a unique MD5 hash for a bet."""
        unique_str = f"{info.get('match')}|{info.get('market')}|{info.get('details')}"
        return hashlib.md5(unique_str.encode('utf-8')).hexdigest()

    async def _send_telegram(self, message: str):
        """Sends an async notification to all configured Telegram chats."""
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        async with aiohttp.ClientSession() as session:
            for chat_id in self.telegram_chat_ids:
                try:
                    payload = {"chat_id": chat_id.strip(), "text": message, "parse_mode": "Markdown"}
                    async with session.post(url, data=payload) as resp:
                        if resp.status != 200:
                            logger.error(f"Telegram failed for {chat_id}: {resp.status}")
                except Exception as e:
                    logger.error(f"Telegram connection error: {e}")

    async def _update_sheet(self, info: dict):
        """Appends a new row to Google Sheets."""
        if not self.enable_gsheets or not self.spreadsheet:
            return

        try:
            worksheet = self.spreadsheet.worksheet(self.gs_worksheet_name)
            # Row format: [ID (auto), Date, Sport, Market, Details, Match, Odds Old, Odds Boost]
            row_data = [
                len(worksheet.get_all_values()), # Simple ID
                datetime.now().strftime("%d/%m/%Y"),
                info['sport'],
                info['market'],
                info['details'],
                info['match'],
                info['odds_old'],
                info['odds_new'],
                "", "", "", "" # Placeholders for analysis columns
            ]
            worksheet.append_row(row_data, value_input_option='USER_ENTERED')
            logger.info(f"Google Sheet updated for: {info['match']}")
        except Exception as e:
            logger.error(f"Google Sheets update failed: {e}")

    async def _setup_browser(self, p):
        """Configures and launches the Playwright browser instance."""
        self.browser = await p.chromium.launch(
            headless=False, # Set to True for production/server environments
            args=['--no-sandbox', '--disable-gpu', '--window-size=1920,1080']
        )
        self.context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        # Evasion technique: mask webdriver property
        await self.context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.page = await self.context.new_page()

    async def _extract_bet_info(self, container) -> dict:
        """Parses a single HTML container to extract bet details."""
        info = {
            "sport": "Unknown", "details": "N/A", "match": "N/A",
            "market": "N/A", "odds_old": "N/A", "odds_new": "N/A",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        try:
            # Extract Sport ID from icon URL
            icon = container.locator("img.pbb-PopularBet_Icon")
            if await icon.count() > 0:
                src = await icon.first.get_attribute("src")
                match = re.search(r'/(\d+)\.svg$', src or "")
                if match:
                    info["sport"] = SPORT_ICON_MAP.get(match.group(1), f"Sport ID {match.group(1)}")

            # Extract Text Details
            text_el = container.locator(".pbb-PopularBet_Text")
            if await text_el.count(): info["details"] = (await text_el.first.inner_text()).strip()

            match_el = container.locator(".pbb-PopularBet_BetLine")
            if await match_el.count(): info["match"] = (await match_el.first.inner_text()).strip()

            market_el = container.locator(".pbb-PopularBet_MarketName")
            if await market_el.count(): info["market"] = (await market_el.first.inner_text()).strip()

            # Odds (Replace dot with comma for European/Sheet format if needed)
            old_odds = container.locator(".pbb-PopularBet_PreviousOdds")
            if await old_odds.count(): info["odds_old"] = (await old_odds.first.inner_text()).strip().replace('.', ',')

            new_odds = container.locator(".pbb-PopularBet_BoostedOdds")
            if await new_odds.count(): info["odds_new"] = (await new_odds.first.inner_text()).strip().replace('.', ',')

        except Exception as e:
            logger.warning(f"Error extracting bet info: {e}")

        return info

    async def run(self):
        """Main execution loop."""
        logger.info("Starting SuperquoteBot...")
        
        async with async_playwright() as p:
            attempt = 0
            while attempt < 5:
                try:
                    await self._setup_browser(p)
                    logger.info("Browser launched successfully.")
                    attempt = 0 # Reset attempts on success

                    while True:
                        logger.info("Navigating to Bet365...")
                        await self.page.goto("https://www.bet365.it/#/HO/", timeout=60000, wait_until="domcontentloaded")
                        await asyncio.sleep(5) # Allow dynamic content to load

                        # Identify containers
                        containers = []
                        for selector in [".pbb-PopularBetsList > div", ".pbb-SuperBetBoost-parent"]:
                            found = self.page.locator(selector)
                            if await found.count() > 0:
                                count = await found.count()
                                for i in range(count):
                                    containers.append(found.nth(i))
                                break # Stop if we found a valid container group
                        
                        current_cycle_ids = []

                        if containers:
                            logger.info(f"Found {len(containers)} potential bets.")
                            for container in containers:
                                # Validation: Ensure it's a boost
                                if await container.locator(".pbb-SuperBetBoost, .pbb-SuperBoostChevron").count() == 0:
                                    continue

                                data = await self._extract_bet_info(container)
                                if data['match'] == "N/A" or data['odds_new'] == "N/A":
                                    continue

                                bet_id = self._generate_id(data)
                                current_cycle_ids.append(bet_id)
                                data['active'] = True

                                # --- Logic: New Bet Found ---
                                if bet_id not in self.active_superquotes:
                                    logger.info(f"✨ NEW BET: {data['match']} ({data['odds_new']})")
                                    
                                    msg = (f"✨ *NEW SUPERQUOTE* ✨\n\n"
                                           f"⚽ {data['sport']}\n🆚 {data['match']}\n"
                                           f"📊 {data['market']}\n📝 {data['details']}\n"
                                           f"📉 {data['odds_old']} ➡ 📈 *{data['odds_new']}*")
                                    
                                    await self._send_telegram(msg)
                                    await self._update_sheet(data)
                                    
                                    self.active_superquotes[bet_id] = data
                                    self.history[bet_id] = data
                                
                                # Update timestamp for existing bets
                                else:
                                    self.history[bet_id]['timestamp'] = data['timestamp']
                                    self.history[bet_id]['active'] = True

                        # --- Logic: Bet Removed ---
                        removed_ids = [bid for bid in self.active_superquotes if bid not in current_cycle_ids]
                        for bid in removed_ids:
                            bet = self.active_superquotes.pop(bid)
                            self.history[bid]['active'] = False
                            logger.info(f"❌ BET REMOVED: {bet['match']}")
                            
                            msg = (f"❌ *SUPERQUOTE ENDED*\n\n"
                                   f"🆚 {bet['match']}\n📉 {bet['odds_old']} ➡ {bet['odds_new']}")
                            await self._send_telegram(msg)

                        # Save and Heartbeat
                        self._save_history()
                        if self.healthcheck_url:
                            try:
                                requests.get(self.healthcheck_url, timeout=10)
                            except requests.RequestException:
                                pass

                        # Wait for next cycle
                        wait_time = random.uniform(70, 110)
                        logger.info(f"Cycle complete. Sleeping for {wait_time:.1f}s...")
                        await asyncio.sleep(wait_time)

                except Exception as e:
                    logger.error(f"Critical Loop Error: {e}")
                    # Screenshot on error
                    try:
                        if self.page: await self.page.screenshot(path=f"error_{datetime.now().timestamp()}.png")
                    except: pass
                    
                    if self.browser: await self.browser.close()
                    attempt += 1
                    await asyncio.sleep(30 * attempt)

if __name__ == "__main__":
    try:
        bot = SuperquoteBot()
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.critical(f"Fatal startup error: {e}")
