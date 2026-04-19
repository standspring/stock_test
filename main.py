# ==========================================================
# [main.py] - ?뙚 100% ?듯빀 ?꾩꽦蹂??뙚
# ?좑툘 ??二쇱꽍 諛??뚯씪紐??쒓린???덈? 吏?곗? 留덉꽭??
# ?뮕 [V24.10] ?붾젅洹몃옩 API ?듭떊 ??꾩븘??TimedOut) 諛⑹뼱 諛?而ㅻ꽖??? 理쒖쟻???댁떇 ?꾨즺
# ?뮕 [V24.11 ?섏닠] VolatilityEngine ?숈쟻 ?곌껐 諛?TelegramController ?섏〈??二쇱엯
# ?뮕 [V24.15 ??섏닠] V_VWAP ?뚮윭洹몄씤 ?섏〈??100% ?곴뎄 ?곸텧 諛?2? 肄붿뼱 泥댁젣 ?뺣┰
# ?뮕 [V24.20 ?⑥튂] ????덊띁?곗떛(SOXX/SOXL) ?명봽??諛??ㅻ깄???뚯씠?꾨씪??利앹꽕
# ?슚 [V25.19 ?ロ뵿?? EST/KST ??꾩〈 ?쇱슜???곕Ⅸ ?ㅼ?以꾨쭅 ?ㅼ옉??諛⑹뼱 (紐낆떆????꾩〈 二쇱엯)
# ?슚 [V25.19 ?ロ뵿?? ????덊띁?곗떛(TICKER_BASE_MAP) ?꾩뿭 怨듭쑀 ?뚯씠?꾨씪???꾨꼍 ?뺣┰
# ?? [V27.00 ?먭? ?낅뜲?댄듃 ?쇱슦???댁떇] ?붾젅洹몃옩 ?몃뱾??猷⑦봽??'update' 紐낅졊??怨듭떇 ?깅줉 ?꾨즺
# ?슚 [V27.11 洹몃옖???섏닠] 肄뷀뙆?쇰읉 ?⑹옉 - asyncio.Lock ?고???遺뺢눼 諛⑹뼱, NaN ?ㅽ뙋 諛???갑???대갚 李⑤떒, 
# 肄쒕뱶 ?ㅽ?????뭾 ?쒖뼱(first=30) 諛??듯빀 ??꾩〈(America/New_York) ?뚯씠?꾨씪??援ъ텞
# ?슚 [V27.12 ?ロ뵿?? ?ш퀎????꾩〈 ?댁쨷 ?寃??먯쿇 李⑤떒, ?댁쨷 ?숆린??遺뺢눼 諛⑹? 諛?FD 理쒖쟻???댁떇
# MODIFIED: [V28.27 洹몃옖???섏닠] 濡쒓렇 ?뚯씪紐??앹꽦 ??KST(?쒕쾭 ?쒓컙) ?섏〈???꾨㈃ ?뚭컖 諛?EST(誘멸뎅 ?숇?) ??꾩〈 ?쎌삩?쇰줈 ?붾쾭源??뚰렪???곴뎄 李⑤떒
# ==========================================================

import os
import logging
import datetime
import pytz
import asyncio
import math # ?슚 [?섏닠 ?꾨즺] NaN 寃利앹슜 math 紐⑤뱢 異붽?
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from dotenv import load_dotenv

from config import ConfigManager
from broker import KoreaInvestmentBroker
from paper_broker import PaperBroker
from strategy import InfiniteStrategy
from telegram_bot import TelegramController

from queue_ledger import QueueLedger
from strategy_reversion import ReversionStrategy
from volatility_engine import VolatilityEngine

from scheduler_core import (
    scheduled_token_check,
    scheduled_auto_sync_summer,
    scheduled_auto_sync_winter,
    scheduled_force_reset,
    scheduled_self_cleaning,
    get_target_hour,
    perform_self_cleaning
)
from scheduler_trade import (
    scheduled_regular_trade,
    scheduled_sniper_monitor,
    scheduled_vwap_trade,
    scheduled_vwap_init_and_cancel,  
    scheduled_after_market_lottery  
)

TICKER_BASE_MAP = {
    "SOXL": "SOXX",
    "TQQQ": "QQQ",
    "TSLL": "TSLA",
    "FNGU": "FNGS",
    "BULZ": "FNGS"
}

if not os.path.exists('data'): os.makedirs('data')
if not os.path.exists('logs'): os.makedirs('logs')

load_dotenv() 

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
try:
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID")) if os.getenv("ADMIN_CHAT_ID") else None
except ValueError:
    ADMIN_CHAT_ID = None

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
CANO = os.getenv("CANO")
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD", "01")
BROKER_MODE = str(os.getenv("BROKER_MODE", "LIVE") or "LIVE").strip().upper()
PAPER_START_CASH = float(os.getenv("PAPER_START_CASH", "100000") or 100000)

# ?슚 [?섏닠 ?꾨즺] ADMIN_CHAT_ID ?꾨씫 ??臾듭뼵?섑뻾(Silent Zombie) 遊?援щ룞 ?먯쿇 李⑤떒
if not all([TELEGRAM_TOKEN, APP_KEY, APP_SECRET, CANO, ADMIN_CHAT_ID]):
    print("??[移섎챸???ㅻ쪟] .env ?뚯씪??遊?援щ룞 ?꾩닔 ??TELEGRAM_TOKEN, APP_KEY, APP_SECRET, CANO, ADMIN_CHAT_ID)媛 ?꾨씫?섏뿀?듬땲?? 遊뉗쓣 醫낅즺?⑸땲??")
    exit(1)

# ?슚 [V28.27 ?섏닠 ?꾨즺] 濡쒓렇 ?뚯씪紐???꾩〈 EST ?쎌삩
est_tz_log = pytz.timezone('US/Eastern')
log_filename = f"logs/bot_app_{datetime.datetime.now(est_tz_log).strftime('%Y%m%d')}.log"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

async def scheduled_volatility_scan(context):
    """
    10:20 EST (?뺢퇋??媛쒖옣 50遺??? 寃⑸컻.
    ???醫낅ぉ?ㅼ쓽 HV? ?뱀씪 VXN???곗궛?섏뿬 ?곕???硫붿씤 ?붾㈃??1-Tier 釉뚮━???ㅽ봽
    """
    app_data = context.job.data
    cfg = app_data['cfg']
    base_map = app_data.get('base_map', TICKER_BASE_MAP)
    
    print("\n" + "=" * 60)
    print("?뱢 [?먯쑉二쇳뻾 蹂?숈꽦 ?ㅼ틪 ?꾨즺] (10:20 EST ?ㅻ깄??")
    
    active_tickers = cfg.get_active_tickers()
            
    if not active_tickers:
        print("?뱤 ?꾩옱 ?댁슜 以묒씤 醫낅ぉ???놁뒿?덈떎.")
    else:
        briefing_lines = []
        vol_engine = VolatilityEngine()
        
        for ticker in active_tickers:
            target_base = base_map.get(ticker, ticker)
            try:
                weight_data = await asyncio.to_thread(vol_engine.calculate_weight, target_base)
                raw_weight = weight_data.get('weight', 1.0) if isinstance(weight_data, dict) else weight_data
                real_weight = float(raw_weight)
                
                # ?슚 [?섏닠 ?꾨즺] NaN/Inf 寃곗륫移?移⑦닾 ??臾댁“嫄?以묐┰(1.0) ?대갚 ?곸슜?섏뿬 怨듦꺽 ?ㅽ뙋 ?먯쿇 李⑤떒
                if not math.isfinite(real_weight):
                    raise ValueError(f"鍮꾩젙???섑븰 ?섏튂 ?곗텧: {real_weight}")
            except Exception as e:
                # ?슚 [?섏닠 ?꾨즺] ?먮윭 ????갑??諛고똿(0.85/1.15) 湲덉?. 臾댁“嫄?以묐┰(1.0) ?덉쟾留덉쭊 媛뺤젣 ?곸슜
                logging.warning(f"[{ticker}] 蹂?숈꽦 吏???곗텧 ?ㅽ뙣. 以묐┰ ?덉쟾留덉쭊(1.0) 媛뺤젣 ?곸슜: {e}")
                real_weight = 1.0 
                
            status_text = "OFF 沅뚯옣" if real_weight <= 1.0 else "ON 沅뚯옣"
            if ticker != target_base: briefing_lines.append(f"{ticker}({target_base}): {real_weight:.2f} ({status_text})")
            else: briefing_lines.append(f"{ticker}: {real_weight:.2f} ({status_text})")
            
        print(f"?뱤 [?먯쑉二쇳뻾 吏?? {' | '.join(briefing_lines)} (?곸꽭 寃뚯씠吏: /mode)")
    print("=" * 60 + "\n")

# ?슚 [?섏닠 ?꾨즺] ?뚯씠??3.10+ ?명솚?깆쓣 ?꾪빐 ?대깽??猷⑦봽 ?대??먯꽌 asyncio.Lock()???덉쟾?섍쾶 ?앹꽦?섎뒗 肄쒕갚
async def post_init(application: Application):
    tx_lock = asyncio.Lock()
    application.bot_data['app_data']['tx_lock'] = tx_lock
    application.bot_data['bot_controller'].tx_lock = tx_lock

def main():
    TARGET_HOUR, season_msg = get_target_hour()
    cfg = ConfigManager()
    latest_version = cfg.get_latest_version() 
    
    print("=" * 60)
    print(f"?? ?깆넄猷⑦듃 ?ㅻ끂?곕낵 ????붿쭊 {latest_version} (珥덇꼍??2? 肄붿뼱 ?꾪궎?띿쿂 ?묒옱)")
    print(f"?뱟 ?좎쭨 ?뺣낫: {season_msg}")
    print("Auto sync schedule: 08:30 (summer) / 09:30 (winter)")
    print(f"?썳截?1-Tier ?먯쑉二쇳뻾 吏???ㅼ틪 ?湲?以?.. (留ㅼ씪 10:20 EST 寃⑸컻)")
    print("=" * 60)
    
    perform_self_cleaning()
    
    # ?곷떒?먯꽌 ADMIN_CHAT_ID ?좏슚??寃?щ? 留덉낀?쇰?濡?臾댁“嫄??명똿
    cfg.set_chat_id(ADMIN_CHAT_ID)
    
    if BROKER_MODE == "PAPER":
        broker = PaperBroker(APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD, initial_cash=PAPER_START_CASH)
        logging.warning(f"[PaperBroker] PAPER mode enabled. Real orders are disabled. Starting paper cash: ${PAPER_START_CASH:,.2f}")
    else:
        broker = KoreaInvestmentBroker(APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD)
    strategy = InfiniteStrategy(cfg)
    queue_ledger = QueueLedger()
    strategy_rev = ReversionStrategy()
    
    # ?슚 [?섏닠 ?꾨즺] tx_lock? ?숆린 ?⑥닔??main()???꾨땶 鍮꾨룞湲?post_init?먯꽌 ?앹꽦?⑸땲??
    bot = TelegramController(
        cfg, broker, strategy, tx_lock=None, 
        queue_ledger=queue_ledger, strategy_rev=strategy_rev
    )
    
    # ?슚 [?섏닠 ?꾨즺] IANA ?쒖? ??꾩〈 ?뚯씠?꾨씪???뺣┰
    kst = pytz.timezone('Asia/Seoul')
    est = pytz.timezone('America/New_York')
    
    app_data = {
        'cfg': cfg, 'broker': broker, 'strategy': strategy, 
        'queue_ledger': queue_ledger, 'strategy_rev': strategy_rev,  
        'bot': bot, 'tx_lock': None, 'base_map': TICKER_BASE_MAP,
        'tz_kst': kst, 'tz_est': est # ??꾩〈 ?꾩뿭 怨듭쑀
    }

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .connect_timeout(30.0)
        .pool_timeout(30.0)
        # MODIFIED: [?⑥씪 愿由ъ옄 遊??섍꼍??留욎떠 FD(?뚯씪 ?붿뒪?щ┰?? ?꾩닔 諛?OS ?먯썝 怨좉컝 諛⑹?瑜??꾪빐 而ㅻ꽖?????512?먯꽌 8濡?理쒖쟻??
        .connection_pool_size(8)
        .post_init(post_init) # ?슚 Lock ?앹꽦???꾪븳 ???곌껐
        .build()
    )
    
    app.bot_data['app_data'] = app_data
    app.bot_data['bot_controller'] = bot
    
    for cmd, handler in [
        ("start", bot.cmd_start), ("record", bot.cmd_record), ("history", bot.cmd_history), 
        ("sync", bot.cmd_sync), ("settlement", bot.cmd_settlement), ("seed", bot.cmd_seed), 
        ("ticker", bot.cmd_ticker), ("mode", bot.cmd_mode), ("reset", bot.cmd_reset), 
        ("version", bot.cmd_version), ("update", bot.cmd_update)
    ]:
        app.add_handler(CommandHandler(cmd, handler))
        
    app.add_handler(CallbackQueryHandler(bot.handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
    
    jq = app.job_queue
    
    # 1. ?쒖뒪??愿由??ㅼ?以꾨윭 (core)
    for tt in [datetime.time(7,0,tzinfo=kst), datetime.time(11,0,tzinfo=kst), datetime.time(16,30,tzinfo=kst), datetime.time(22,0,tzinfo=kst)]:
        jq.run_daily(scheduled_token_check, time=tt, days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # MODIFIED: [?댁쨷 ?붽퀬 ?숆린??諛⑹뼱] ?ш퀎??TARGET_HOUR) 湲곗???留욎떠 ?щ쫫/寃⑥슱 ?숆린???ㅼ?以꾩쓣 ???섎굹留??깅줉
    SYNC_HOUR = 8 if TARGET_HOUR == 17 else 9
    SYNC_FUNC = scheduled_auto_sync_summer if TARGET_HOUR == 17 else scheduled_auto_sync_winter
    jq.run_daily(SYNC_FUNC, time=datetime.time(SYNC_HOUR, 30, tzinfo=kst), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # MODIFIED: [?댁쨷 ?寃?諛⑹뼱] 17??18?쒓? 臾댁“嫄?紐⑤몢 ?깅줉?섎뒗 踰꾧렇瑜??쒓굅?섍퀬 TARGET_HOUR ?⑥씪 ?щ’?먮쭔 ??珥덇린???깅줉
    jq.run_daily(scheduled_force_reset, time=datetime.time(TARGET_HOUR, 0, tzinfo=kst), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)
        
    jq.run_daily(scheduled_volatility_scan, time=datetime.time(10, 20, tzinfo=est), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # 2. ?ㅼ쟾 ?꾪닾 留ㅻℓ ?ㅼ?以꾨윭 (trade)
    # MODIFIED: [?댁쨷 ?寃?諛⑹뼱] 17:05/18:05 ?숈떆 諛쒖궗(Double-buying) 踰꾧렇瑜??먯쿇 李⑤떒?섍퀬 TARGET_HOUR?먮쭔 ?뺢퇋???寃??ㅼ?以??깅줉
    jq.run_daily(scheduled_regular_trade, time=datetime.time(TARGET_HOUR, 5, tzinfo=kst), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    jq.run_daily(scheduled_vwap_init_and_cancel, time=datetime.time(15, 30, tzinfo=est), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)

    # ?슚 [?섏닠 ?꾨즺] 肄쒕뱶 ?ㅽ?????뭾 諛⑹뼱: 遊?援щ룞 ??30珥???泥??ㅽ뻾(first=30)
    jq.run_repeating(scheduled_sniper_monitor, interval=60, first=30, chat_id=ADMIN_CHAT_ID, data=app_data)
    jq.run_repeating(scheduled_vwap_trade, interval=60, first=30, chat_id=ADMIN_CHAT_ID, data=app_data)
    
    jq.run_daily(scheduled_after_market_lottery, time=datetime.time(16, 5, tzinfo=est), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)

    jq.run_daily(scheduled_self_cleaning, time=datetime.time(6, 0, tzinfo=kst), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()
