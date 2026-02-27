import sqlite3
import pandas as pd
import time
import random
#strip rubbish strings in titles
import re
#for grabbing urls with various different special characters
import urllib.parse
#scraping
from playwright.sync_api import sync_playwright

def extract_metric(page, label_text):
    #searching for text label instead of css elements
    #found within inspect element
    #3 diff ways 
            #ignoring spaces and within sibling follow to right
    xpaths = [
        # label in "container" <td> with value next
        
        f"//span[normalize-space(text())='{label_text}']/parent::*/following-sibling::*[1]//span[contains(@class, 'sales-data__price')]",
        # finding it immediately instead of jumping side by side
        f"//span[normalize-space(text())='{label_text}']/following-sibling::*[contains(@class, 'sales-data__price')]",
        # either jumping side by side, finding immediately, or above or below
        f"//span[normalize-space(text())='{label_text}']/..//*[contains(@class, 'sales-data__price')]"
    ]
    #for each method
    for xpath in xpaths:
        loc = page.locator(f"xpath={xpath}").first
        if loc.count() > 0:
            return loc
    return None

def process_item(page, url):
    #initializing 
    total_sold = 0
    avg_daily_sold = 0.0

    try:
    #goes to the url, waits till base of html is loaded
    #instead of default wait until load
    #dont need images + other info to load just need html
        page.goto(url, wait_until="domcontentloaded")
        
        # important: If we fall back to a search URL (because `baseline_prices` didn't have URLs),
        # need to click into the first product result, with exact item names as described in baseline_price table in tcg would always be first product
        if "/search/" in page.url or "q=" in url:
            try:
                page.wait_for_selector('.search-result', timeout=8000)
                first_link = page.locator('.search-result a').first
                #if exists basically
                if first_link.count() > 0:
                    #gets url
                    href = first_link.get_attribute("href")
                    if href:
                        if not href.startswith('http'):
                            #adds it within url if didnt have - unlikely
                            href = "https://www.tcgplayer.com" + href
                        page.goto(href, wait_until="domcontentloaded")
            except Exception as e:
                print(f"!!!! Oops Failed to navigate to product: {e}")
                
        # timeout cause waiting for sales data to load.. takes a while sometimes
        try:
            page.wait_for_selector('.sales-data__price', state="visible", timeout=10000)
        except:
            # if > 10 seconds, just try below after another 5 second timeout
            try:
                page.wait_for_load_state("networkidle", timeout=5000)
            except:
                pass

        #extracting finally

        #Total Sold
        ts_loc = extract_metric(page, "Total Sold:")
        if ts_loc:
            text = ts_loc.inner_text().strip()
            # extract just int
            #delete anything not int
            extracted_digits = re.sub(r'[^\d]', '', text)
            if extracted_digits:
                total_sold = int(extracted_digits)

        # from xpath
        #saw rounding on screen, if you click html has actual number, extracting actual number not rounded
        ads_loc = extract_metric(page, "Avg. Daily Sold:")
        #just incase.. use what is on screen if not title
        if ads_loc:
            title_attr = ads_loc.get_attribute("title")
            # fallbackto inner text if title attribute fails
            val_text = title_attr if title_attr else ads_loc.inner_text()
            val_text = val_text.strip()
            # floats not ints cause can be 0 < x < 1
            extracted_digits = re.sub(r'[^\d.]', '', val_text)
            if extracted_digits:
                try:
                    avg_daily_sold = float(extracted_digits)
                except ValueError:
                    #pass anyways
                    pass

    except Exception as e:
        print(f" Oops.. Exception during scraping: {e}")
        
    return total_sold, avg_daily_sold


def main():
    #for fun
    print("="*60)
    print("Lets rock! Starting TCGPlayer Liquidity Scraper")
    print("="*60)

    db_path = "tcg_inventory.db"
    
    try:
        conn = sqlite3.connect(db_path)
    except Exception as e:
        print(f"Database connection error: {e}")
        return
        
    # load arbitrage profit desc top 100 deals!!
    try:
        query = "SELECT * FROM arbitrage_opportunities ORDER BY profit_margin DESC"
        arb_df = pd.read_sql_query(query, conn)
        arb_df = arb_df.head(100)
    except Exception as e:
        print(f"Error loading arbitrage_opportunities: {e}")
        conn.close()
        return
        
    ### DONT EVEN NEED BELOW
    try:
        bp_df = pd.read_sql_query("SELECT * FROM baseline_prices", conn)
    except Exception as e:
        print(f"Error loading baseline_prices: {e}")
        bp_df = pd.DataFrame()
        
    print(f"Loaded {len(arb_df)} top arbitrage deals and {len(bp_df)} baseline prices.")
    
    # Check if 'url' actually exists in baseline_prices. 
    # (If not, our fallback logic kicks in seamlessly using TCGplayer's search engine)
    has_url_col = 'url' in bp_df.columns
    if not has_url_col:
        print("[Notice] No 'url' column found in baseline_prices. Will use TCGplayer dynamic search as fallback.")

    url_map = {}
    if has_url_col:
        for _, row in bp_df.iterrows():
            if pd.notna(row.get('set_name')) and pd.notna(row.get('url')):
                url_map[row['set_name']] = row['url']
    ###DONT EVEN NEED ABOVE
    #initially attempted to correlate tcgplayer url.. no need at all

    #empty lists for liquidity
    total_sales_list = []
    avg_daily_list = []
    
    # playwright stealth
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            #prevent browser cash incase of memory limits
            #disabling navigator.webdriver for spam-detection
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage"
            ]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        #new individualized chrome page
        page = context.new_page()
        
        # for each deal out of 100
        for idx, row in arb_df.iterrows():
            tcg_title = row.get('tcg_title', '')
            #just to print tracking for fun. indexed out of total
            print(f"[{idx+1}/{len(arb_df)}] Scraping liquidity for: {tcg_title}")
            ##DOESNT APPLY ## ## BELOW DOESNT APPLY##
            target_url = url_map.get(tcg_title)
            ##ABOVE DOESNT APPLY##

            # auto-search exact product URL 
            if not target_url or pd.isna(target_url):
                #title into encoded title with urlib.parse to then search through target url in loop
                encoded_title = urllib.parse.quote_plus(tcg_title)
                target_url = f"https://www.tcgplayer.com/search/pokemon/product?q={encoded_title}&view=grid"
            #returned values assigned
            #total sold to ts avg daily sold to ads
            ts, ads = process_item(page, target_url)
            print(f" ---- Total Sold: {ts} | Avg. Daily Sold: {ads}")
            #append
            total_sales_list.append(ts)
            avg_daily_list.append(ads)
            
            # more jitter for spam-detection
            delay = random.uniform(4.0, 8.0)
            time.sleep(delay)
        #close individualized
        browser.close()
        
    # add back to df into columns, if empty then 0
    arb_df["total_sold"] = total_sales_list
    arb_df["avg_daily_sold"] = avg_daily_list
    
    # new table
    #refresh upon running
    table_name = "enriched_top_deals"
    try:
        arb_df.to_sql(table_name, conn, if_exists="replace", index=False)
        #lott of space
        print("\n" + "="*60)
        print(f"SUCCESS! Enriched max {len(arb_df)} deals with correlated liquidity metrics.")
        print(f"Data saved to the '{table_name}' table.")
        print("="*60)
    except Exception as e:
        print(f"Error saving to database '{table_name}': {e}")
        
    conn.close()

if __name__ == "__main__":
    main()
