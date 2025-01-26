import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json
from textblob import TextBlob
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.exc import IntegrityError
import xlsxwriter
import logging
import re
import traceback
import concurrent.futures
import time

# Database and Logging Setup
Base = declarative_base()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Database Models (unchanged from previous version)
class Sneaker(Base):
    __tablename__ = "sneakers"

    name = sa.Column(sa.String, primary_key=True)
    brand_name = sa.Column(sa.String)
    price = sa.Column(sa.Float)
    review_count = sa.Column(sa.Integer)
    sku = sa.Column(sa.String)
    url = sa.Column(sa.String)


class Analysis(Base):
    __tablename__ = "analysis"

    name = sa.Column(sa.String, primary_key=True)
    brand = sa.Column(sa.String)
    positive_reviews = sa.Column(sa.Integer, default=0)


class ZapposScraper:
    BASE_URL = "https://www.zappos.com/men-lifestyle-sneakers/CK_XARC81wEYz-4BwAEC4gIEAQIDGA.zso"

    def __init__(self, session, db_session):
        self.session = session
        self.db_session = db_session
        self.sneakers = []

    async def extract_total_pages(self, url):
        async with self.session.get(url) as response:
            html = await response.text()

            soup = BeautifulSoup(html, "html.parser")
            pagination = soup.select_one(".eo-z")
            if pagination:
                match = re.search(r"1 of (\d+)", pagination.text)
                return int(match.group(1)) if match else 1
            return 1

    def extract_product_data(self, html):
        soup = BeautifulSoup(html, "html.parser")
        products = soup.select('script[type="application/ld+json"]')

        for product_script in products:
            try:
                product_data = json.loads(product_script.string)
                if product_data.get("@type") == "Product":
                    sneaker = Sneaker(
                        name=product_data.get("name"),
                        brand_name=product_data.get("brand", {}).get("name"),
                        price=float(product_data.get("offers", {}).get("price", 0)),
                        review_count=product_data.get("aggregateRating", {}).get(
                            "reviewCount"
                        ),
                        sku=product_data.get("sku"),
                        url=product_data.get("url"),
                    )
                    self.sneakers.append(sneaker)
            except Exception as e:
                logger.error(f"Error extracting product: {e}")

    async def scrape_listing_page(self, url):
        async with self.session.get(url) as response:
            html = await response.text()
            self.extract_product_data(html)
            return

    #
    async def scrape_reviews(self, sneaker):
        if not sneaker.sku or not sneaker.review_count:
            return 0

        base_review_url = (
            f"https://www.zappos.com/product/review/{sneaker.sku}/page/{{}}"
        )

        # Extract total review pages
        async with self.session.get(base_review_url.format(1)) as first_page:
            html = await first_page.text()
            soup = BeautifulSoup(html, "html.parser")

            pagination = soup.select_one(".eo-z")
            total_review_pages = 1
            if pagination:
                match = re.search(r"1 of (\d+)", pagination.text)
                total_review_pages = int(match.group(1)) if match else 1

        async def fetch_review_page(page):
            # await asyncio.sleep(0.1)  # Minimal delay
            async with self.session.get(base_review_url.format(page)) as response:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")

                page_positive_reviews = 0
                reviews = soup.select("div.Ba-z")
                for review in reviews:
                    verified_purchase = review.select_one("._p-z")
                    if not verified_purchase:
                        continue

                    review_text = review.select_one(
                        'div.Sp-z[itemprop="reviewBody"] .mq-z.nq-z'
                    )
                    if review_text:
                        sentiment = TextBlob(review_text.text).sentiment.polarity
                        if sentiment > 0 and "return" not in review_text.text.lower():
                            page_positive_reviews += 1
                return page_positive_reviews

        # Create tasks for all pages
        tasks = [fetch_review_page(page) for page in range(1, total_review_pages + 1)]

        # Use asyncio.gather with a limit
        sem = asyncio.Semaphore(10)

        async def bounded_fetch(task):
            async with sem:
                return await task

        page_results = await asyncio.gather(*[bounded_fetch(task) for task in tasks])

        positive_reviews = sum(page_results)
        print(f"Name: {sneaker.name} \nPositive reviews: {positive_reviews}")

        return positive_reviews

    def export_to_excel(self):
        # Create workbooks
        workbook = xlsxwriter.Workbook("zappos_sneakers.xlsx")

        # Sneakers Workbook
        sneakers_sheet = workbook.add_worksheet("Sneakers")
        headers = ["Name", "Brand", "Price", "Review Count", "SKU", "URL"]
        for col, header in enumerate(headers):
            sneakers_sheet.write(0, col, header)

        for row, sneaker in enumerate(self.sneakers, 1):
            sneakers_sheet.write(row, 0, sneaker.name)
            sneakers_sheet.write(row, 1, sneaker.brand_name)
            sneakers_sheet.write(row, 2, sneaker.price)
            sneakers_sheet.write(row, 3, sneaker.review_count)
            sneakers_sheet.write(row, 4, sneaker.sku)
            sneakers_sheet.write(row, 5, sneaker.url)

        # Analysis Workbook
        analysis_sheet = workbook.add_worksheet("Analysis")
        analysis_headers = ["Name", "Brand", "Positive Reviews"]
        for col, header in enumerate(analysis_headers):
            analysis_sheet.write(0, col, header)

        analysis_data = self.db_session.query(Analysis).all()
        for row, analysis in enumerate(analysis_data, 1):
            analysis_sheet.write(row, 0, analysis.name)
            analysis_sheet.write(row, 1, analysis.brand)
            analysis_sheet.write(row, 2, analysis.positive_reviews)

        # Most Sold Workbook
        most_sold_sheet = workbook.add_worksheet("Most Sold")
        most_sold_headers = ["Name", "Brand", "Positive Reviews"]
        for col, header in enumerate(most_sold_headers):
            most_sold_sheet.write(0, col, header)

        # Order by positive reviews
        most_sold_data = (
            self.db_session.query(Analysis)
            .order_by(Analysis.positive_reviews.desc())
            .limit(20)
            .all()
        )

        for row, analysis in enumerate(most_sold_data, 1):
            most_sold_sheet.write(row, 0, analysis.name)
            most_sold_sheet.write(row, 1, analysis.brand)
            most_sold_sheet.write(row, 2, analysis.positive_reviews)

        workbook.close()

    async def run(self):
        try:
            async with aiohttp.ClientSession() as session:
                self.session = session

                # Clear any existing sneakers
                self.sneakers.clear()

                # Scrape listing page
                total_pages = await self.extract_total_pages(self.BASE_URL)
                print(f"Total pages: {total_pages}")

                # Scrape additional pages
                for page in range(1, total_pages):
                    listing_url = f"{self.BASE_URL}?p={page-1}"
                    await self.scrape_listing_page(listing_url)
                    # break

                print(f"Total sneakers scraped: {len(self.sneakers)}")

                # Save sneakers to database
                print("Saving sneakers to database... \n")
                for sneaker in self.sneakers:
                    try:
                        self.db_session.merge(sneaker)
                    except Exception as e:
                        print(f"Error saving sneaker {sneaker.name}: {e}")
                        traceback.print_exc()
                        self.db_session.rollback()

                # Commit after all sneakers are processed
                self.db_session.commit()

                # Analyze reviews
                print("Analyzing reviews...")
                for sneaker in self.sneakers:
                    try:
                        positive_reviews = await self.scrape_reviews(sneaker)
                        analysis = Analysis(
                            name=sneaker.name,
                            brand=sneaker.brand_name,
                            positive_reviews=positive_reviews,
                        )
                        self.db_session.merge(analysis)
                    except Exception as e:
                        print(f"Error analyzing reviews for {sneaker.name}: {e}")
                        traceback.print_exc()
                        self.db_session.rollback()
                    # break

                # Final commit
                self.db_session.commit()

                # Export to Excel
                print("\n\nExporting data to excel... \n")
                self.export_to_excel()

                print("Done.")

        except Exception as e:
            print(f"Unexpected error in run method: {e}")
            traceback.print_exc()


def main():
    # Database setup
    engine = sa.create_engine("sqlite:///zappos_sneakers.db")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        scraper = ZapposScraper(None, session)
        asyncio.run(scraper.run())


if __name__ == "__main__":
    main()
