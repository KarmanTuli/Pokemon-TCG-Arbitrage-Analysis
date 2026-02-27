#async for simultaneous processing to load pages
import asyncio
#jitter for clicking on chrome pages
import random
import sqlite3
#run dates
import datetime
import pandas as pd
#to see script open chrome
from playwright.async_api import async_playwright
#ensuing safe from bot-detection, good IP
from playwright_stealth import Stealth

#using standard chrome agent for scraping, with stealth and random to make sure bot-detection prevention and jitters 
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"

# reading screen
async def extract_cards_from_current_page(page):
    try:
        # ensuring screen is reading data before scraping
        await page.locator('.search-result').first.wait_for(timeout=10000)
    except Exception:
        print("No search results found or timed out waiting for them.")
        #if nothing gets scraped
        return []

    #storage bin
    results = []
    
    # stores all listings
    cards = await page.locator('.search-result').all()
    #for terminal processing ease of reading
    print(f"Found {len(cards)} products on this page.")
    
    for card in cards:
        try:
            # extracting title from offer up, stripped
            #extracting where 
            title_element = card.locator('.product-card__title')
            #extract string
            title = await title_element.inner_text()
            title = title.strip()
            
            # turning $ to float for price of listing
            #css locator for specific style class found inspect element
            price_locator = card.locator('.product-card__market-price--value')
            #if price (always there but incase of outlier)
            if await price_locator.count() > 0:
                price_text = await price_locator.first.inner_text()
                cleaned_text = price_text.replace('$', '').replace(',', '').strip()
                price = float(cleaned_text) if cleaned_text else None
            #unlikely for most part, every tcgplayer listing should have price
            else:
                price = None
            #append title, price, rundate    
            results.append({
                "item_name": title,
                "market_price": price,
                "timestamp": datetime.datetime.now().isoformat()
            })
        #if somehow title isnt there, playwright doesnt scrape
        except Exception as e:
            print(f"Error parsing card: {e}")
            
    return results

#begin scrape
async def run_scraper():
    results = []
    #stealth needed incase of IP spam-detection
    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(
            #watch scrape
            headless=False, 
            #prevent browser cash incase of memory limits
            #disabling navigator.webdriver for spam-detection
            args=[
                "--disable-blink-features=AutomationControlled",
                #use main memory incase of big page load
                "--disable-dev-shm-usage"
            ]
        )
        #isolated window needed to not interfere with other windows
        context = await browser.new_context(
            #"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            user_agent=USER_AGENT,
            #ease of reading
            viewport={"width": 1920, "height": 1080},
            device_scale_factor=1,
            #desktop
            is_mobile=False,
            has_touch=False
        )
        page = await context.new_page()
        
        # specific search for Pokemon Sealed Products category
        start_url = "https://www.tcgplayer.com/search/pokemon/product?productLineName=pokemon&ProductTypeName=Sealed+Products&page=1"
        print(f"Loading initial URL: {start_url}")
        await page.goto(start_url, wait_until="domcontentloaded")
        #loop
        page_num = 1
        
        while True:
            print(f"\n--- Scraping Page {page_num} ---")
            
            # 4 seconds to render anti-spam
            await asyncio.sleep(4) 
            
            # 2. reading page and listings within
            page_results = await extract_cards_from_current_page(page)
            #incase empty page in category
            if not page_results:
                print(f"No results found on page {page_num}.")
                break

            #appends to results    
            results.extend(page_results)
            print(f"Total items extracted so far: {len(results)}.")
            
            #aria label for next button on tcgplayer next page
            next_button = page.get_by_label("Next page")
            
            # if no button
            if await next_button.count() == 0 or await next_button.get_attribute('aria-disabled') == 'true':
                print(f"Next button is missing and or disabled on page {page_num}.")
                break 
                
            print("Clicking to next page!")
            
            # for clicks of aria label
            await next_button.click()
            page_num += 1
            
            #jitter range 
            delay = random.uniform(5, 9)
            print(f"Waiting for {delay:.2f} seconds for jitter and server load...")
            #pause script during this period
            await asyncio.sleep(delay)
            
        await browser.close()
        
    return results

def main():
    print("Starting stealth scraper for TCGplayer...")
    
    #asynchronous 
    results = asyncio.run(run_scraper())
    #if failure
    if not results:
        print("No prices extracted.")
        return
        
    #load into pandas, into new db
    df = pd.DataFrame(results)
    db_filename = "tcg_inventory.db"

    #quick db refresh
    try:
        # 2. Connect to the database
        conn = sqlite3.connect(db_filename)
        
        #if exsits in table, refresh, want fresh data for table incase of new item drops
        df.to_sql("baseline_prices", conn, if_exists="replace", index=False)
        
        print(f"\n Table 'baseline_prices' has been refreshed.")
        print(f"Total new products stored: {len(df)}")
        
        conn.close()
        #incase holding file or viewing ensure noted to runner 
    except sqlite3.OperationalError as e:
        print(f"\n DATABASE RUN FAILED: {e}")

if __name__ == "__main__":
    main()