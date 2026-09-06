# MCP tool has no rate limit but must be synchronous/sequential
# Explicit high-value UK airports for every occupancy + duration
# Month split only if an airport slice hits the 10k truncation limit


import asyncio
import json
import time
import hashlib
import sqlite3
from pathlib import Path
from datetime import datetime,date

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

DB_PATH=Path(__file__).resolve().parent/"holidays.db"
MCP_URL="https://mcp.travelsupermarket.com/mcp"
PAGE_SIZE=20
MAX_OFFSET=10000
RETRY_DELAYS_S=[0.5,1,2,4]

DURATIONS=["1","2","3","4","5","6","7","8","9","10","11","12","13","14"]
MONTHS=[str(i) for i in range(1,13)]
DEPARTURE_AIRPORTS=["MAN","BHX","LGW","LHR","LTN","STN","EMA","BRS","NCL","LBA","LPL","EDI","GLA","BFS"]
OCCUPANCIES=[{"adults":str(a),"children":str(c),"infants":"0"} for a in range(1,3) for c in range(6)]

BASE_SEARCHES=len(OCCUPANCIES)*len(DEPARTURE_AIRPORTS)*len(DURATIONS)


search_count=total_raw=total_stored=total_added=total_date_rejected=total_maxed=0

def elapsed(start):
    s=int(time.perf_counter()-start);m=s//60;h=m//60
    return f"{h}h {m%60}m {s%60}s" if h else f"{m}m {s%60}s" if m else f"{s}s"

def within_one_year(v):
    if not v:return False
    try:d=datetime.fromisoformat(str(v).replace("Z","+00:00")).date()
    except Exception:
        try:d=date.fromisoformat(str(v)[:10])
        except Exception:return False
    start=date.today()
    try:end=start.replace(year=start.year+1)
    except ValueError:end=start.replace(year=start.year+1,day=28)
    return start<=d<=end

def offer_key(o):
    v=[o.get("advertiserName"),o.get("hotelTtiCode") or o.get("hotelName"),o.get("departureAirportIata") or o.get("departureAirport"),o.get("departureDate"),o.get("returnDate"),o.get("duration"),o.get("boardBasisCode") or o.get("boardBasis"),o.get("adults"),o.get("children"),o.get("infants")]
    return hashlib.sha256("|".join("" if x is None else str(x) for x in v).lower().encode()).hexdigest()

def save_offer(db,o):
    key=offer_key(o);ts=datetime.now().astimezone().isoformat(timespec="milliseconds");cur=db.execute("SELECT first_seen,price_per_person,total_price,price_history FROM TSM_data WHERE offer_key=?",(key,)).fetchone()
    try:history=json.loads(cur["price_history"] or "[]") if cur else []
    except Exception:history=[]
    if not isinstance(history,list):history=[]
    def num(v):
        try:return float(v)
        except (TypeError,ValueError):return 0
    def integer(v):
        try:return int(float(v))
        except (TypeError,ValueError):return 0
    pp=num(o.get("pricePerPerson"));total=num(o.get("totalPrice"))
    if not cur or num(cur["price_per_person"])!=pp or num(cur["total_price"])!=total:history.append({"ts":ts,"price_per_person":pp,"total_price":total})
    db.execute("""INSERT INTO TSM_data (offer_key,provider,brand_logo_url,hotel_name,hotel_tti_code,departure_airport,departure_airport_iata,departure_date,return_date,duration,board_basis,adults,children,infants,destination_name,resort,region,country,star_rating,review_score,review_count,facilities,image_url,price_per_person,total_price,price_history,source_deep_link,first_seen,last_seen,last_validated,active)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
    ON CONFLICT(offer_key) DO UPDATE SET provider=excluded.provider,brand_logo_url=excluded.brand_logo_url,hotel_name=excluded.hotel_name,hotel_tti_code=excluded.hotel_tti_code,departure_airport=excluded.departure_airport,departure_airport_iata=excluded.departure_airport_iata,departure_date=excluded.departure_date,return_date=excluded.return_date,duration=excluded.duration,board_basis=excluded.board_basis,adults=excluded.adults,children=excluded.children,infants=excluded.infants,destination_name=excluded.destination_name,resort=excluded.resort,region=excluded.region,country=excluded.country,star_rating=excluded.star_rating,review_score=excluded.review_score,review_count=excluded.review_count,facilities=excluded.facilities,image_url=excluded.image_url,price_per_person=excluded.price_per_person,total_price=excluded.total_price,price_history=excluded.price_history,source_deep_link=excluded.source_deep_link,last_seen=excluded.last_seen,active=1""",(key,o.get("advertiserName"),o.get("brandLogoUrl"),o.get("hotelName"),o.get("hotelTtiCode"),o.get("departureAirport"),o.get("departureAirportIata"),o.get("departureDate"),o.get("returnDate"),integer(o.get("duration")) or None,o.get("boardBasisCode") or o.get("boardBasis"),integer(o.get("adults")),integer(o.get("children")),integer(o.get("infants")),o.get("destinationName"),o.get("resort"),o.get("region"),o.get("country"),num(o.get("starRating")) or None,num(o.get("reviewScore")) or None,integer(o.get("reviewCount")) or None,json.dumps(o.get("facilities") or []),o.get("imageUrl"),pp or None,total or None,json.dumps(history,separators=(",",":")),o.get("deepLinkUrl"),cur["first_seen"] if cur else ts,ts,None))
    return cur is None

async def main(wait=0.1):
    global search_count, total_raw, total_stored, total_added, total_date_rejected, total_maxed
    started=time.perf_counter();
    exists=DB_PATH.exists();
    start_db_bytes=DB_PATH.stat().st_size if exists else 0
    total_data_bytes=0
    db=sqlite3.connect(DB_PATH,timeout=5);
    db.row_factory=sqlite3.Row
    db.executescript("""PRAGMA journal_mode=WAL;PRAGMA synchronous=NORMAL;PRAGMA busy_timeout=5000;
    CREATE TABLE IF NOT EXISTS TSM_data (id INTEGER PRIMARY KEY AUTOINCREMENT,offer_key TEXT NOT NULL UNIQUE,provider TEXT,brand_logo_url TEXT,hotel_name TEXT,hotel_tti_code TEXT,departure_airport TEXT,departure_airport_iata TEXT,departure_date TEXT,return_date TEXT,duration INTEGER,board_basis TEXT,adults INTEGER,children INTEGER,infants INTEGER,destination_name TEXT,resort TEXT,region TEXT,country TEXT,star_rating REAL,review_score REAL,review_count INTEGER,facilities TEXT,image_url TEXT,price_per_person REAL,total_price REAL,price_history TEXT NOT NULL DEFAULT '[]',source_deep_link TEXT,first_seen TEXT NOT NULL,last_seen TEXT NOT NULL,last_validated TEXT,active INTEGER NOT NULL DEFAULT 1);
    CREATE INDEX IF NOT EXISTS idx_tsm_active ON TSM_data(active);CREATE INDEX IF NOT EXISTS idx_tsm_price ON TSM_data(price_per_person);CREATE INDEX IF NOT EXISTS idx_tsm_departure_date ON TSM_data(departure_date);CREATE INDEX IF NOT EXISTS idx_tsm_provider ON TSM_data(provider);""");
    db.commit()
    print(f"{'DB FOUND' if exists else 'DB CREATED'} | {DB_PATH}")
    print(f"DB READY | OCCUPANCIES:{len(OCCUPANCIES)} | AIRPORTS:{len(DEPARTURE_AIRPORTS)} | DURATIONS:{len(DURATIONS)} | BASE SEARCHES:{BASE_SEARCHES}")
    try:
        async with streamable_http_client(MCP_URL) as streams:
            async with ClientSession(streams[0],streams[1]) as session:
                await session.initialize();
                print("MCP READY");
                base_index=0
                for o in OCCUPANCIES:
                    for airport in DEPARTURE_AIRPORTS:
                        for duration in DURATIONS:
                            base_index+=1;
                            searches=[None];
                            search_pos=0
                            while search_pos<len(searches):
                                month=searches[search_pos];
                                search_pos+=1;
                                search_count+=1;
                                search_started=time.perf_counter();
                                label=f"BASE {base_index}/{BASE_SEARCHES}" if month is None else f"MONTH {month}"
                                args={"adults":o["adults"],"children":o["children"],"infants":o["infants"],"departureAirport":airport,"duration":duration,"limit":PAGE_SIZE,"offset":0}
                                if month is not None:args["departureMonth"]=month
                                offset=pages=raw=stored=added=date_rejected=data_bytes=0;
                                offers=[]
                                while offset<=MAX_OFFSET:
                                    args["offset"]=offset
                                    offers=[]
                                    try:
                                        for attempt in range(len(RETRY_DELAYS_S)+1):
                                            try:
                                                request_bytes=len(json.dumps(args,separators=(",",":"),ensure_ascii=False).encode("utf-8"))
                                                result=await session.call_tool("search-holidays",arguments=args)
                                                if getattr(result,"isError",False):raise RuntimeError("\n".join(getattr(x,"text","") for x in (getattr(result,"content",[]) or []) if getattr(x,"text","")) or "Tool returned isError=true")
                                                structured=getattr(result,"structuredContent",None)
                                                if structured is None:
                                                    dumped=result.model_dump(mode="python") if hasattr(result,"model_dump") else {}
                                                    structured=dumped.get("structuredContent") or dumped.get("structured_content")
                                                if hasattr(structured,"model_dump"):structured=structured.model_dump(mode="python")
                                                if not isinstance(structured,dict) or not isinstance(structured.get("offers"),list):raise RuntimeError(f"MCP RESPONSE MISSING OFFERS | {str(result)[:1500]}")
                                                response_bytes=len(json.dumps(structured,separators=(",",":"),ensure_ascii=False,default=str).encode("utf-8"))
                                                data_bytes+=request_bytes+response_bytes
                                                total_data_bytes+=request_bytes+response_bytes
                                                offers=structured["offers"];
                                                break
                                            except Exception as err:
                                                if attempt>=len(RETRY_DELAYS_S):raise
                                                delay=RETRY_DELAYS_S[attempt];
                                                print(f"\nRETRY | {err} | {int(delay*1000)}ms");
                                                await asyncio.sleep(delay)
                                    except Exception as fatal_err:
                                        print(f"\n[PAGE ERROR] Skipping page at offset {offset} due to unrecoverable error: {fatal_err}")
                                        if wait>0:await asyncio.sleep(wait)
                                        break
                                    if wait>0:await asyncio.sleep(wait)
                                    pages+=1;
                                    raw+=len(offers);
                                    total_raw+=len(offers)
                                    for offer in offers:
                                        if not within_one_year(offer.get("departureDate")):date_rejected+=1;total_date_rejected+=1;continue
                                        if save_offer(db,offer):added+=1;total_added+=1
                                        stored+=1;
                                        total_stored+=1
                                    db.commit();
                                    print(f"\r\033[2KCRAWL | {label} | SEARCH:{search_count} | {o['adults']}A {o['children']}C {o['infants']}I | {airport} | {duration}N | PAGE:{pages} | OFFSET:{offset} | RAW:{raw} | UPDATED:{stored} | NEW:{added} | DATA:{data_bytes/1024:,.1f}KB | TOTAL:{total_data_bytes/1024/1024:,.2f}MB | TIME:{elapsed(search_started)}",end="",flush=True)
                                    if len(offers)<PAGE_SIZE:break
                                    offset+=PAGE_SIZE
                                maxed=offset>MAX_OFFSET and len(offers)==PAGE_SIZE
                                print(f"\r\033[2KDONE | {label} | SEARCH:{search_count} | {o['adults']}A {o['children']}C {o['infants']}I | {airport} | {duration}N | PAGES:{pages} | RAW:{raw} | UPDATED:{stored} | NEW:{added} | DATA:{data_bytes/1024:,.1f}KB | TOTAL:{total_data_bytes/1024/1024:,.2f}MB | TIME:{elapsed(search_started)}")
                                if maxed:
                                    total_maxed+=1
                                    if month is None:print("MAX OFFSET | SPLITTING INTO 12 MONTHS");searches.extend(MONTHS)
                                    else:print(f"WARNING | MONTH {month} STILL HIT MAX OFFSET")
                total=db.execute("SELECT COUNT(*) FROM TSM_data").fetchone()[0];
                active=db.execute("SELECT COUNT(*) FROM TSM_data WHERE active=1").fetchone()[0]
                db.commit();db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                end_db_bytes=DB_PATH.stat().st_size
                captured_kb=max(0,end_db_bytes-start_db_bytes)/1024
                db_kb=end_db_bytes/1024
                print(f"CRAWL COMPLETE | UNIQUE DEALS:{total} | ACTIVE:{active} | BASE SEARCHES:{BASE_SEARCHES} | ACTUAL SEARCHES:{search_count} | RAW:{total_raw} | UPDATED:{total_stored} | NEW:{total_added} | DATE REJECT:{total_date_rejected} | MAXED:{total_maxed} | DATA:{total_data_bytes/1024/1024:,.2f}MB | CAPTURED:{captured_kb:,.1f}KB | DB:{db_kb:,.1f}KB | TIME:{elapsed(started)}")
    finally:
        db.close()

if __name__=="__main__":
    try:asyncio.run(main())
    except KeyboardInterrupt:print("\nCRAWLER STOPPED")
    except Exception as err:print(f"\nCRAWLER FAILED | {type(err).__name__}: {err}");raise