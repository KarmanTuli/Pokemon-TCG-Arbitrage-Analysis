#gemini 2.5 flash key
#environment variable to not have key within file for security
#if needed to run, grab env example file and must create own key with gemini studio
#utilized flash due to low token costs + max token daily cap which is 1 million
import os
import sqlite3
import pandas as pd
import time
#reading back to python
import json
#enforces gemini to return in schema described
from pydantic import BaseModel
from google import genai
#import gemmini with httpoptions for timeout incase
from google.genai import types
from google.genai.types import HttpOptions # Required for setting timeouts

#os for key in terminal
api_key = os.environ.get("GEMINI_API_KEY")
#reminder for me
if not api_key:
    raise ValueError("GEMINI_API_KEY not found. Please set it before running.")

#gemini flash known to possibly be stuck, especially with large token usage. 30 second timeout
client = genai.Client(
    api_key=api_key,
    http_options=HttpOptions(timeout=30000) 
)

DB_NAME = "tcg_inventory.db"

#defining floor prices to exclude trash or irrelevant listings these prices would be seen as nonsensible in today's market
MIN_PRICE_THRESHOLDS = {
    'Pokemon Booster Box': 160.00, 
    'Pokemon Elite Trainer Box': 50.00,
    'Pokemon ETB': 50.00,
    'Pokemon Booster Bundle': 28.00,
    'Pokemon UPC': 100.00,
    'Pokemon Premium Collection': 25.00,
    'Pokemon Collection': 15.00,
    'Pokemon Tin': 10.00,
    'Pokemon Blister': 5.00,
    'Pokemon Booster Pack': 4.00, 
    'Pokemon Case': 100.00,
    'Pokemon Display': 50.00,
    'Pokemon Sleeved Booster': 5.00,
    'Pokemon Blister': 5.00
}

#
#pydantic to transform json into data output with correct structure
#define types
class CleanListing(BaseModel):
    url: str
    item_name: str
    clean_price: float
    location: str
    distance_tier: int
#ready to result in only clean listings
def pre_filter_data(df):
    print(f"Initial raw rows from OfferUp: {len(df)}")
    
    # removes n/a prices as defined previously
    df = df[df['price'] != 'N/A'].copy()
    
    # float
    df['clean_price'] = df['price'].replace(r'[\$,]', '', regex=True).astype(float)
    
    #recognized offerup has emojis so removing emojis, otherwise gemini has struggle with receiving json 
    df['item_name'] = df['item_name'].replace(r'[^\x00-\x7F]+', '', regex=True)
    
    #list of clean listings
    valid_rows = []
    #offerup raw data index to walk through each row
    for index, row in df.iterrows():
        #to indicate price floors for each row
        term = row['search_term']
        price = row['clean_price']
        #append if greater than min price
        if term in MIN_PRICE_THRESHOLDS and price >= MIN_PRICE_THRESHOLDS[term]:
            valid_rows.append(row)
    #into dataframe
    filtered_df = pd.DataFrame(valid_rows)
    #cleaned amount
    print(f"Rows remaining after applying price floor: {len(filtered_df)}")
    return filtered_df
#took longer to run but more important to ensure each run follows rules
#chunks for processing
def clean_with_gemini(chunk):
    #initial run wasnt efficient in terms of token count, trimmed to necessary columns as gemini is focused on item name and price thresholds
    data_to_send = chunk[['item_name', 'clean_price']].to_dict(orient='records')
    #prompt made for gemini api to review
    #most common things to exclude. listings in tcg often may have sealed product name but then include keywords below indicating not sealed but rather on those below
    #triple quotes for formatting for gemini's ease of understanding
    prompt = f"""
    Review these Pokemon listings. Return ONLY official SEALED products.
    Exclude: single cards, slabs, codes, toys, empty boxes, or acrylic cases.
    Data: {json.dumps(data_to_send)}
    """
    #tracking time of batch run
    start_time = time.time()
    
    # gemini outputs in JSON array 
    response = client.models.generate_content(
        model='gemini-2.5-flash',
        #prompt above
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            #schema:     url: str item_name: str clean_price: float location: str distance_tier: int
            response_schema=list[CleanListing],
            #low for data cleaning. strict logic. no creativity needed here. just applying to rules
            temperature=0.1 
        )
    )
    #stop time
    duration = time.time() - start_time
    print(f" Received response in {duration:.2f} seconds.")
    #returns string
    #dumpted json string in format for item name: x and clean price: x
    return json.loads(response.text)

def main():
    print("Grabbing raw OfferUp data...")
    conn = sqlite3.connect(DB_NAME)
    try:
        df = pd.read_sql_query("SELECT * FROM raw_offerup_listings", conn)
    except Exception as e:
        #if select error
        print(f"Error reading offerup_scraper: {e}")
        return
    finally:
        #ensuring no corruption.. db always terminated
        conn.close()

    ##same price floor filter
    filtered_df = pre_filter_data(df)
    #if nothing passed from price filter - as precaution
    if filtered_df.empty:
        print("No rows left after pre-filtering. Stopping.")
        return

    # Step 2: The LLM Transformation Layer
    print("\n🚀 Sending batches to Gemini API for spam removal...")
    all_clean_listings = []
    
    #initially tried 40, then 20, then 10. Small enough for flash to focus on each row and also less batch processing wait time if simultaneously processing more at the same time
    chunk_size = 10 
    #go from 0 to x in chunk and keeps "indexing" until entire table processed
    chunks = [filtered_df[i:i + chunk_size] for i in range(0, filtered_df.shape[0], chunk_size)]
    #for chunk # and data within. keeping track 
    for i, chunk in enumerate(chunks):
        print(f"\nProcessing batch {i+1} of {len(chunks)}...")
        
        try:
            # Send the compressed batch
            clean_chunk = clean_with_gemini(chunk)
            
            # #taking url from original chunk back into result. to save on tokens. need to have link for deal in the dashboard
            for item in clean_chunk:
                original = chunk[chunk['item_name'] == item['item_name']].iloc[0]
                item['url'] = original['url']
                item['location'] = original['location']
                item['distance_tier'] = original['distance_tier']
                
            all_clean_listings.extend(clean_chunk)
            #if batch failes, switch to go row by row instead of batch processing
        except Exception as e:
            print(f" Batch cannot be processed (likely JSON Error). Switching to individual processing..")
            #if batch fails (generally due to json error, previously was emoji but for also external factors)
            for _, individual_row in chunk.iterrows():
                try:
                    #row by row
                    single_item_df = pd.DataFrame([individual_row])
                    res = clean_with_gemini(single_item_df)
                    #adding all to dictionary at once
                    if res:
                        res[0].update({
                            'url': individual_row['url'], 
                            'location': individual_row['location'], 
                            'distance_tier': individual_row['distance_tier']
                        })
                        #appending those which failed batch, back into final list
                        all_clean_listings.extend(res)
                    time.sleep(1) 
                except Exception:
                    #reads listing out
                    print(f"Skipping 1 undetermined item: {individual_row['item_name'][:30]}...")
        
        # for rate limits, read that too much spam might suspend or ban key -- just to be safe
        time.sleep(4)
        
    print(f"\n WOW.. ! Gemini successfully processed {len(all_clean_listings)} sealed items!")
    
    if not all_clean_listings:
        print("No clean listings returned from the Flash. Exiting.")
        return

    # load the clean data back to dn
    clean_df = pd.DataFrame(all_clean_listings)
    #drop duplicate listings as url is good unique identifier
    clean_df = clean_df.drop_duplicates(subset=['url'])
    #if exists replace.. refresh for each run
    conn = sqlite3.connect(DB_NAME)
    clean_df.to_sql("clean_offerup_listings", conn, if_exists="replace", index=False)
    print(f"Successfully saved {len(clean_df)} unique items to 'clean_offerup_listings' table.")
    conn.close()

if __name__ == "__main__":
    main()