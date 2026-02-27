I created an end-to-end data pipeline and analytical dashboard to identify arbitrage opportunities within the local marketplace on OfferUp relative to Pokemon Trading Card Game sealed market prices. Within this project I have leveraged stealth scraping and Gemini 2.5 Flash to transform thousands of unstructured marketplace listings into actionable opportunities. By integrating fuzzy-matching logic with custom, niche business rules, the pipeline ultimately creates a subset of high-margin deals, achieving a 12.1% yield of validated arbitrage opportunities.

Pokemon has now turned into the highest-grossing media franchise of all time, with the 2026 market offering arbitrage opportunities and potential profit for thousands of English sealed products. I have personally pursued this myself, and have gone through several challenges - specifically with OfferUp - in finding relevant, valid listings with profit potential. This project automates the painful process of manually hunting through thousands of junk listings into a near-instant profit and ROI analysis across thousands of unique products sliced by set names, cities, distance, and liquidity, ultimately creating a data-driven effort. 

The data pipeline captures market prices from TCGPlayer and thousands of listings from OfferUp, then match the two to identify these opportunities. Based on the top 100 opportunities liquidity factors such as Total Daily Sold and Avg. Daily Sold within last three months are included for knowledgeable decision-making. All of this data is stored in a tcg_inventory SQLite database, which ultimately connects live to PowerBI for real-time decision making and analysis.  

NOTE: Must need copy of personal Gemini 2.5 Flash key to run gemini_cleaner script. 

I have included all the processes, ran in order below:

**TCGPlayer Market Price Scrape:**
baseline_prices.py

Scrapes prices of 2,000+ English Sealed Pokemon TCG Products, and the according price for each product directly from TCGPlayer.com. TCGPlayer contains the biggest dataset for market data for Trading Card Game products.

Creates baseline_prices table in tcg_inventory.db

**OfferUp Listing Scraper**
offerup_scraper.py

Scrapes listings of English sealed Pokemon Products within products in a 10, 20, 30 mile radius from my zipcode. Aiming for local deals and eventually measuring effort/distance relative to profit.

Creates raw_offerup_listings table in tcg_inventory.db

**Gemini 2.5 Flash Efficient Cleaning**
Gemini_cleaner.py

Cleans 6000+ local Pokemon sealed listings with the use of an effective Gemini 2.5 Flash prompt - individually scanning each row ( Ex: !!!POKEMON 151 BUNDLE FOR SALE!! ---> Pokemon 151 Bundle.. next step converts this Pokemon 151 Bundle to Pokemon 151 Booster Bundle - the standardized name to then compare to TCGPlayer).

Creates clean_offerup_listings table in tcg_inventory.db

**Arbitrage Opportunities Identified**
Arbitrage_engine.py

Run script to refresh arbitrage_opportunities (table stored in SQLite database), discovering Profit Margin, Return on Investment, City Location, Distance Tier, URL, Item Name, and Price Difference for each OfferUp listing.

Creates arbitrage_opportunities table in tcg_inventory.db

**Liquidity for Top 100 Deals**
Liquidity_scraper.py 

With a base of the arbitrage opportunities identified, this script scrapes TCGPlayer once more for the liquidity of these deals, ensuring measured decision making when potentially securing a deal. When searching for potential profit, it is best to stick with higly liquid items. This brings in Total Sold and Average Daily Sold in last 3 months.


Creates enriched_top_deals table in tcg_inventory.db

_**PowerBI Dashboard titled "Pokemon TCG Arbitrage"** has a live connection to these tables, enabling in-depth analysis._
