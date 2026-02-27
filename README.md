I created an end-to-end data pipeline to identify arbitrage opportunities within the local marketplace on OfferUp relative to Pokemon Trading Card Game market prices. utilized stealth scraping, Gemini 2.5 Flash for human-like cleaning, fuzzy-matching based on key logic and business rules, and liquidity analysis based on product names.   Pokemon has now turned into the highest-grossing media franchise of all time, with the 2026 market offering arbitrage opportunities and potential profit for thousands of English sealed products. I have personally pursued this myself, and have gone through several challenges - specifically with OfferUp - in finding relevant, valid listings with profit potential. This data pipeline captures market prices from TCGPlayer and thousands of listings from OfferUp, then match the two to identify opportunities. Based on the top 100 opportunities, I have also added liquidity factors, such as Total Daily Sold and Avg. Daily Sold within last three months. All of this data is stored in a tcg_inventory SQLite database, which ultimately connects live to PowerBI for real-time decision making and analysis.  NOTE: Must need Gemini 2.5 Flash key to run gemini_cleaner script. Have not included my gemini key for safety!
In the files uploaded - as of 2/27/2026 - I have included all python files, ran in order below:

**TCGPlayer Market Price Scrape:**
baseline_prices.py

Scrapes prices of 2,000+ English Sealed Pokemon TCG Products, and the according price for each product. TCGPlayer contains the biggest dataset for market data for Trading Card Game products.

**OfferUp Listing Scraper**
offerup_scraper.py

Scrapes listings of English sealed Pokemon Products within products in a 10, 20, 30 mile radius from my zipcode. Aiming for local deals and eventually measuring effort/distance relative to profit.

**Gemini 2.5 Flash Efficient Cleaning**
Gemini_cleaner.py

Cleans 6000+ local Pokemon sealed listings ( Ex: !!!POKEMON 151 BUNDLE FOR SALE!! ---> Pokemon 151 Bundle.. next step converts this Pokemon 151 Bundle to Pokemon 151 Booster Bundle - the standardized name to then compare to TCGPlayer).

**Arbitrage Opportunities Identified**
Arbitrage_engine.py

Run script to refresh arbitrage_opportunities (table stored in SQLite database), discovering Profit Margin, Return on Investment, City Location, Distance Tier, URL, Item Name, and Price Difference for each OfferUp listing.

**Liquidity for Top 100 Deals**
With a base of the arbitrage opportunities identified, this script scrapes TCGPlayer once more for the liquidity of these deals, ensuring measured decision making when potentially securing a deal. When searching for potential profit, it is best to stick with higly liquid items. This brings in Total Sold and Average Daily Sold in last 3 months.

ALL DATA STORED IN INTERNAL SQLite DATABASE


_**PowerBI Dashboard titled "Pokemon TCG Arbitrage"** has a live connection to these tables, enabling in-depth analysis._
