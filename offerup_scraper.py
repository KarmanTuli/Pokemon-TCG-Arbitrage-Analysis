#async for simultaneous processing to load pages
import asyncio
#jitter for clicking on chrome pages
import random
#for grabbing urls with various different special characters
import urllib.parse
import sqlite3
import pandas as pd
#strip rubbish strings in titles
import re
#ensuing safe from bot-detection, good IP and script open in chrome simultaneously
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# keywords of all main english sealed. note - excluding Binder as binder is synonymous with a binder full of cards. Would be due for error here
#excluding build and battles and decks, holiday calendars due to no margin and demand
KEYWORDS = [
    'Pokemon Booster Box', 'Pokemon Elite Trainer Box', 'Pokemon ETB', 
    'Pokemon Booster Bundle', 'Pokemon UPC', 'Pokemon Premium Collection', 
    'Pokemon Collection', 'Pokemon Tin', 'Pokemon Blister', 'Pokemon Booster Pack', 
    'Pokemon Case', 'Pokemon Display', 'Pokemon Sleeved Booster', 'Pokemon Blister'
]
#setting distance tiers available in offerup into storage. Keep note anything > 30 in offerup is considered Max. We would include folks 100s even 1000s miles away
DISTANCES = [10, 20, 30]
DB_NAME = "tcg_inventory.db"
#my zip for actual real usage
ZIP_CODE = "91709" 

#using standard chrome agent for scraping, with stealth and random to make sure bot-detection prevention and jitters 
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"

#asynchronous extract
async def extract_listings(page, keyword, distance):
    """Getting product's item_name, price, location, and url"""
    try:
        #html grabbing <a> href address, waiting 5 seconds 
        await page.wait_for_selector('a[href*="/item/detail/"]', timeout=5000)
    except Exception:
        pass 
    #all offerup listings with html having item detail. All valid listings have this
    elements = await page.query_selector_all('a[href*="/item/detail/"]')
    #if empty listing
    if len(elements) == 0:
        #if keyword shows nothing
        print(f"No items found for '{keyword}' at {distance} miles.")
        return []
    #storing
    listings = []
    #loop for each listing
    for el in elements:
        href = await el.get_attribute("href")
        #skip if href link not found
        if not href:
            continue
        #ensuring grabbing frmo offerup    
        full_url = href if href.startswith("http") else f"https://offerup.com{href}"
        
        #buckets for each loop
        price = "N/A"
        item_name = ""
        location = "Unknown"
        
        #grabbing listing name aria metadata
        aria_label = await el.get_attribute("aria-label")
        
        if aria_label:
            # looks for a $ followed by numbers/commas/decimals, cleans with re
            import re
            #dollar sign following range of possibly numbers and commas included. If decimals (not really needed for offerup) but just incase
            price_match = re.search(r'\$([0-9,]+(?:\.[0-9]{2})?)', aria_label)
            #appending dollar back
            if price_match:
                price = "$" + price_match.group(1)
                
            # offerup always puts city after in. Finding city for location
            loc_match = re.search(r' in (.*?)\s*$', aria_label)
            if loc_match:
                location = loc_match.group(1).strip()
                
            #if before dollar sign strip left side and grab.. title of listing
            if '$' in aria_label:
                item_name = aria_label.split('$')[0].strip()
            else:
                item_name = aria_label.strip()
        #BACKUP
        # if no aria label hence no price, item -  scan chromium browser ui
        #visible text
        if price == "N/A" or not item_name:
            #text scrape
            text_content = await el.inner_text()
            #splitting line
            lines = [line.strip() for line in text_content.split('\n') if line.strip()]
            #if na make equal to scanned line
            for line in lines:
                if '$' in line and price == "N/A":
                    price = line
                elif len(line) > 3 and "ago" not in line.lower() and not item_name and "Shipping" not in line:
                    item_name = line
            if not item_name and lines:
                item_name = lines[0]
        #columns needed
        listings.append({
            "url": full_url,
            "item_name": item_name,
            "price": price,
            "location": location, 
            "distance_tier": distance,
            "search_term": keyword
        })

    return listings

async def scrape_offerup():
    print("Starting OfferUp Scraper...")
    all_listings = []

    # stealth for anti spam detection
    async with Stealth().use_async(async_playwright()) as p:
        #launch visibly
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            #"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            user_agent=USER_AGENT,
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()
        #matrix search for each radius
        for keyword in KEYWORDS:
            for distance in DISTANCES:
                #transform for url
                query = urllib.parse.quote(keyword)
                #run through url for specific searching per each distance defined  and my defined zip code
                url = f"https://offerup.com/search?q={query}&distance={distance}&vzip={ZIP_CODE}"
                
                print(f"Accessing: {url}")
                #no next page on offerup, continue scrolling with 6 second load
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                except Exception as e:
                    print(f"ERROR Loading {url} failed: {e}")
                    continue

                # scroll 5 times safe enough to grab all listings
                #typically 5 times should cover recently listed items, past 5 would just have aged listings or not relevant
                for i in range(5):
                    #javascript to scroll to bottom
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    #2 seconds
                    await asyncio.sleep(2)
                
                await asyncio.sleep(1)
                #add to above function
                listings = await extract_listings(page, keyword, distance)
                #append
                all_listings.extend(listings)
               
                print(f"Extracted {len(listings)} items.")
                #range of jitter for asyncio
                delay = random.uniform(5.5, 12.5) 
                await asyncio.sleep(delay)

        await browser.close()
        
    return all_listings

def main():
    results = asyncio.run(scrape_offerup())
    #if error not appending to results
    if not results:
        print("No prices extracted. Exiting without saving to database.")
        return
        
    # --- THE PANDAS SAVE FIX ---
    df = pd.DataFrame(results)
    
    # Enforce Unique URL: Drop any duplicate listings we scraped across different searches
    df = df.drop_duplicates(subset=['url'])
    print(f"\nFinal Unique Listings Extracted: {len(df)}")
    #if error
    try:
        conn = sqlite3.connect(DB_NAME)
        # replace and store new listings with every refresh, can exclude sold listings
        df.to_sql("raw_offerup_listings", conn, if_exists="replace", index=False)
        print(f"✅ Successfully stored in '{DB_NAME}' (table: raw_offerup_listings).")
        conn.close()
    except sqlite3.OperationalError as e:
        print(f"\n DATABASE ERROR: {e}")

if __name__ == "__main__":
    main()
