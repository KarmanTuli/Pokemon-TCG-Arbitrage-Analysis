import sqlite3
import pandas as pd
#fuzzy string matching - connecting clean listings table with baseline prices table to formulate final arbitrage opportunities
from rapidfuzz import fuzz


#IMPORTANT HERE functions defined as a lot of rules known INTERNALLY within Pokemon
#what should apply if certain listing keywords shown in offerup
    ##which sealed products equate to other sealed products, abbreviations, other keywords, other ways to recognize, etc
    #in a way implementing business logic

#rules to apply if in BOTH listings or exclude
def check_two_way_category(title1, title2, keywords):
    has_1 = any(kw in title1 for kw in keywords)
    has_2 = any(kw in title2 for kw in keywords)
    return has_1 == has_2
#rules to apply if going one way if offerup contains this, tcgplayer listing must contain this - vice versa
#EXAMPLES BELOW
def check_one_way_category(source_title, target_title, source_keywords, target_keywords=None):
    if target_keywords is None:
        target_keywords = source_keywords
    has_source = any(kw in source_title for kw in source_keywords)
    if has_source:
        has_target = any(kw in target_title for kw in target_keywords)
        return has_target
    return True
#ensure they are same
#offerup vs tcg title + set name
def hybrid_match(offerup_title, tcg_title, master_set_names):
    if not isinstance(offerup_title, str) or not isinstance(tcg_title, str):
        return 0
  #standardized casing      
    offerup_title = offerup_title.lower()
    tcg_title = tcg_title.lower()
    offerup_words = offerup_title.split()
    
   #Beginning guardrails
    #majority of valid pokemon tcg listings would have 3 or more words for sellers to correctly advertise item
    #large chance if not, invalid junk

    if len(offerup_words) < 3:
        return 0
        
    #for master set names defined, if more than two in the same listing, reject it. WE WANT INDIVIDUAL itemized listings. Not bulk. 
    #otherwise wont be able to accurately compare to tcg prices
    sets_found = sum(1 for set_name in master_set_names if set_name.lower() in offerup_title)
    if sets_found > 2: 
        return 0
        
    #more rules 

    # if tcgplayer product name says 151, offerup MUST say 151. if set isnt described you cant identify which sealed product
    for set_name in master_set_names:
        set_name_lower = set_name.lower()
        #if in tcg, apply match if not 0
        if set_name_lower in tcg_title:
            if set_name_lower not in offerup_title:
                return 0

    #specific collection prefixes (special sealed products that may not be included in set names)
    collection_prefixes = ["unova", "prismatic", "paldea", "kanto", "johto", "hoenn", "sinnoh", "galar", "alola"]
    for prefix in collection_prefixes:
        #these are NICHE products above. if not included in tcg, take out
        if prefix in tcg_title and prefix not in offerup_title:
            return 0
            
    # specifc for sea and sky collection boxes
    #can be & and
    if not check_one_way_category(tcg_title, offerup_title, ["sea & sky", "sea and sky"], ["sea & sky", "sea and sky"]): return 0

    #ABOVE FOCUSED ON CATEGORIZED PRODUCTS            
    #if included in tcg + offerup for specific sealed product, match. if not TAKE OUT

    #all major rules for pokemon tcg
    if not check_two_way_category(offerup_title, tcg_title, ["case"]): return 0
    if not check_two_way_category(offerup_title, tcg_title, ["pokemon center", "pc etb", "pkc"]): return 0
    if not check_two_way_category(offerup_title, tcg_title, ["upc", "ultra premium"]): return 0
    if not check_two_way_category(offerup_title, tcg_title, ["premium figure"]): return 0
    if not check_two_way_category(offerup_title, tcg_title, ["set of 2", "set of 3", "collection"]): return 0
    if not check_two_way_category(offerup_title, tcg_title, ["display"]): return 0
    if not check_two_way_category(offerup_title, tcg_title, ["3 pack"]): return 0
    if not check_two_way_category(offerup_title, tcg_title, ["2 pack"]): return 0
    if not check_one_way_category(offerup_title, tcg_title, ["bundles"], ["bundle"]): return 0
    if not check_one_way_category(offerup_title, tcg_title, ["tin"]): return 0
    if not check_one_way_category(offerup_title, tcg_title, ["pack"]): return 0
    if not check_one_way_category(offerup_title, tcg_title, ["poster"]): return 0
    if not check_one_way_category(offerup_title, tcg_title, ["pin"]): return 0
    if not check_one_way_category(offerup_title, tcg_title, ["binder"]): return 0
    if not check_one_way_category(offerup_title, tcg_title, ["booster box"]): return 0
    if not check_one_way_category(offerup_title, tcg_title, ["sleeved"]): return 0
    if not check_one_way_category(offerup_title, tcg_title, ["playmat"]): return 0
    
    # specific to multi-pack rules and how they are standardized / generally described across tcg and offerup listings
    if not check_one_way_category(tcg_title, offerup_title, ["2-pack", "2 pack", "two pack","[set of 2]"], ["2-pack", "2 pack", "two pack", "2x", "set of 2", "2-pack blister"]): return 0
    if not check_one_way_category(tcg_title, offerup_title, ["3-pack", "3 pack", "three pack","[set of 3]"], ["3-pack", "3 pack", "three pack", "3x", "set of 3", "3-pack blister"]): return 0
    if not check_one_way_category(tcg_title, offerup_title, ["5-pack", "5 pack", "five pack","[set of 5]"], ["5-pack", "5 pack", "five pack", "5x"]): return 0
    if not check_one_way_category(tcg_title, offerup_title, ["pokemon go", "pokémon go"]): return 0
    
    #same here. different ways to identify specific sealed products
    if not check_one_way_category(tcg_title, offerup_title, ["classic"]): return 0
    if not check_one_way_category(tcg_title, offerup_title, ["etb", "elite trainer"]): return 0
    if not check_one_way_category(tcg_title, offerup_title, ["booster box", "bb"], ["booster box", "bb", "display"]): return 0
    if not check_one_way_category(tcg_title, offerup_title, ["bundle"]): return 0
    
    return fuzz.token_set_ratio(offerup_title, tcg_title)

def main():
    db_path = "tcg_inventory.db"
    #grabbing both tables within db to match
    try:
        conn = sqlite3.connect(db_path)
        offerup_df = pd.read_sql_query("SELECT * FROM clean_offerup_listings", conn)
        tcg_df = pd.read_sql_query("SELECT * FROM baseline_prices", conn)
    except Exception as e:
        print(f"Error accessing database: {e}")
        return
    #set names . both table rows must have to match. only way to individually identify sealed product except for special collections which are specified above
    master_set_names = [
        "Base Set", "Jungle", "Fossil", "Base Set 2", "Team Rocket", "Gym Heroes", "Gym Challenge", 
        "Neo Genesis", "Neo Discovery", "Neo Revelation", "Neo Destiny", "Legendary Collection", 
        "Expedition Base Set", "Aquapolis", "Skyridge", "EX Ruby & Sapphire", "EX Sandstorm", 
        "EX Dragon", "EX Team Magma vs Team Aqua", "EX Hidden Legends", "EX FireRed & LeafGreen", 
        "EX Team Rocket Returns", "EX Deoxys", "EX Emerald", "EX Unseen Forces", "EX Delta Species", 
        "EX Legend Maker", "EX Holon Phantoms", "EX Crystal Guardians", "EX Dragon Frontiers", 
        "EX Power Keepers", "Diamond & Pearl", "Mysterious Treasures", "Secret Wonders", 
        "Great Encounters", "Majestic Dawn", "Legends Awakened", "Stormfront", "Platinum", 
        "Rising Rivals", "Supreme Victors", "Arceus", "HeartGold & SoulSilver", "Unleashed", 
        "Undaunted", "Triumphant", "Call of Legends", "Black & White", "Emerging Powers", 
        "Noble Victories", "Next Destinies", "Dark Explorers", "Dragons Exalted", "Boundaries Crossed", 
        "Plasma Storm", "Plasma Freeze", "Plasma Blast", "Legendary Treasures", "XY", "Flashfire", 
        "Furious Fists", "Phantom Forces", "Primal Clash", "Roaring Skies", "Ancient Origins", 
        "BREAKthrough", "BREAKpoint", "Fates Collide", "Steam Siege", "Evolutions", "Sun & Moon", 
        "Guardians Rising", "Burning Shadows", "Crimson Invasion", "Ultra Prism", "Forbidden Light", 
        "Celestial Storm", "Lost Thunder", "Team Up", "Unbroken Bonds", "Unified Minds", "Cosmic Eclipse", 
        "Rebel Clash", "Darkness Ablaze", "Vivid Voltage", "Battle Styles", 
        "Chilling Reign", "Evolving Skies", "Fusion Strike", "Celebrations", "Generations", 
        "Brilliant Stars", "Astral", "Pokemon GO", "Lost Origin", "Silver Tempest", 
        "Crown Zenith", "Paldea", "Obsidian", "151", "Paradox Rift", 
        "Paldean Fates", "Temporal", "Twilight", "Shrouded", "Stellar Crown", 
        "Surging", "Prismatic", "Journey", "Destined Rivals", "White Flare", 
        "Black Bolt", "Mega Evolution", "Phantasmal", "Ascended", "Perfect Order"
    ]
    
    #matches list
    matches = []
    print(f"Analyzing {len(offerup_df)} local listings against {len(tcg_df)} prices...")
    #iterate through rows for each deal
    #dont need index value actually _
    for _, ou_row in offerup_df.iterrows():
        #for safety
        ou_title = str(ou_row.get("item_name", ""))
        #very much not possible I believe in offerup, but still for safety
        try:
            ou_price = float(ou_row.get("clean_price", 0.0))
        except: continue
        #reset match. one for each item
        #initialize for each record as loop goes through 
        best_match_title = None
        best_score = 0
        best_tcg_price = 0.0
        
        #comparing iterated deal within table to every row in tcgplayer item + price
        for _, tcg_row in tcg_df.iterrows():
            tcg_title = str(tcg_row.get("product_name", tcg_row.get("set_name", "")))
            try:
                tcg_price = float(tcg_row.get("market_price", 0.0))
            except: continue
            
            #set 85 score
            #through trial runs, understanding matches with < 85
            #85 seemed to be threshold where most accuracy, ensuring junk data isnt getting passed
            #even at 75-80 mark i would see sets not matching with one another due to spelling errors/other invalid ways to describe in offerup
            score = hybrid_match(ou_title, tcg_title, master_set_names)
            
            if score >= 85 and score > best_score:
                best_score = score
                best_match_title = tcg_title
                best_tcg_price = tcg_price
                
        # tcg columns into Dictionary - adding offerup records    
        if best_match_title:
             match_data = ou_row.to_dict()
             match_data["tcg_title"] = best_match_title
             match_data["tcg_price"] = best_tcg_price
             match_data["match_score"] = best_score
             #goes through each master_set_names
             matched_set = next((s for s in master_set_names if s.lower() in best_match_title.lower()), "Other/Promo")
             match_data["set_name"] = matched_set
             matches.append(match_data)
             
    if not matches:
        print("Oopsies! 0 matches were found. Check if 'item_name' is valid.")
        conn.close()
        return
    #new dataframe
    matches_df = pd.DataFrame(matches)
    #profit margin is difference of market price to listed price.. potential max profit margin
    matches_df["profit_margin"] = matches_df["tcg_price"] - matches_df["clean_price"]
    #roi as defined is profit divided by listing price. convert into percent. can x100 again in PBI
    matches_df["roi_percentage"] = (matches_df["profit_margin"] / matches_df["clean_price"]) * 100
    matches_df["roi_percentage"] = matches_df["roi_percentage"].replace([float('inf'), float('-inf')], 0.0).fillna(0.0)
    #drop if offerup is less than 50% of market price
    #Almost never would you see for 50% of price unless it is heavily outdated. Not realistic in today's market
    matches_df = matches_df[matches_df["clean_price"] >= matches_df["tcg_price"] * 0.5]
    #drop if roi is more than 150%
    #Almost never would you see for 150% margin. Users tend to check market prices. Assumed to be ruled out due to inaccurate listing/incorrect match
    matches_df = matches_df[matches_df["roi_percentage"] <= 150.0]
    #no point if appending items if ROI is less than 0! Excluding all that we may lose money from
    matches_df = matches_df[matches_df["roi_percentage"] >= 0.0]
    
    #sort by profit
    matches_df = matches_df.sort_values(by="profit_margin", ascending=False)
    #refresh whenever ran
    matches_df.to_sql("arbitrage_opportunities", conn, if_exists="replace", index=False)
    print(f"Success! Found {len(matches_df)} highly accurate matches. All opportunities saved to 'arbitrage_opportunities' table.")
    conn.close()

if __name__ == "__main__":
    main()