# skytrax_spider/spiders/british_airways.py

import scrapy
from loguru import logger
import re

# Configure Loguru logger
logger.add(
    "../../logs/scraper.log",
    rotation="5 MB",
    retention="10 days",
    level="INFO",
    enqueue=True,
    backtrace=True,
    diagnose=True
)

class BritishAirwaysSpider(scrapy.Spider):
    name = "british_airways"
    start_urls = [
        'https://www.airlinequality.com/airline-reviews/british-airways/page/1/'
    ]

    def __init__(self, *args, **kwargs):
        try:
            super().__init__(*args, **kwargs)
            self.total_reviews = 0
            self.max_pages = 10  # Limit to first 10 pages
            logger.info("BritishAirwaysSpider initialized.")
        except Exception as e:
            logger.error(f"Error during initialization: {e}")

    def parse(self, response):
        try:
            logger.info(f"Parsing page: {response.url}")
            reviews = response.css('article.comp_media-review-rated')
            review_count = len(reviews)
            logger.info(f"Found {review_count} reviews on this page.")

            for idx, review in enumerate(reviews, start=1):
                try:
                    review_data = {
                        'title': review.css('h2.text_header::text').get(default='').strip(),
                        'author': review.css('span[itemprop="name"]::text').get(default='').strip(),
                        'date': review.css('meta[itemprop="datePublished"]::attr(content)').get(default='').strip(),
                        'content': ' '.join(review.css('div[itemprop="reviewBody"] *::text').getall()).strip(),
                        'type_of_traveller': review.css('td.review-rating-header.type_of_traveller + td.review-value::text').get(default='').strip(),
                        'seat_type': review.css('td.review-rating-header.cabin_flown + td.review-value::text').get(default='').strip(),
                        'route': review.css('td.review-rating-header.route + td.review-value::text').get(default='').strip(),
                        'date_flown': review.css('td.review-rating-header.date_flown + td.review-value::text').get(default='').strip(),
                        'rating': review.css('div[itemprop="reviewRating"] span[itemprop="ratingValue"]::text').get(default='').strip(),
                        'recommended': review.css('td.review-rating-header.recommended + td.review-value::text').get(default='').strip()
                    }

                    # Check for missing fields and log warnings
                    missing_fields = [key for key, value in review_data.items() if not value]
                    if missing_fields:
                        logger.warning(f"Missing fields in review {idx} on {response.url}: {missing_fields}")

                    self.total_reviews += 1
                    logger.debug(f"Scraped review {self.total_reviews}: {review_data['title']} by {review_data['author']}")

                    yield review_data
                except Exception as e:
                    logger.error(f"Error parsing review {idx} on {response.url}: {e}")
            # Alternative solution for sequential pagination
            # Follow pagination using the XPath selector for 'Next Page'
            # next_page = response.xpath('//article[contains(@class, "comp_reviews-pagination")]//a[text()=">>"]/@href').get()
            # if next_page:
            #     next_page_url = response.urljoin(next_page)
            #     logger.info(f"Following to next page: {next_page_url}")
            #     yield scrapy.Request(next_page_url, callback=self.parse)
            # Extract current page number from the URL using regex
            current_page_match = re.search(r'/page/(\d+)/', response.url)
            if current_page_match:
                current_page = int(current_page_match.group(1))
            else:
                current_page = 1  # Default to 1 if not found

            logger.info(f"Current page number: {current_page}")

            # Determine if the next page should be scraped
            if current_page < self.max_pages:
                next_page = current_page + 1
                next_page_url = f'https://www.airlinequality.com/airline-reviews/british-airways/page/{next_page}/'
                logger.info(f"Following to next page: {next_page_url} (Page {next_page})")
                yield scrapy.Request(next_page_url, callback=self.parse)
            else:
                logger.info(f"Reached the maximum page limit ({self.max_pages} pages). Stopping spider.")
        except Exception as e:
            logger.error(f"Error parsing page {response.url}: {e}")

    def close(self, reason):
        try:
            logger.info(f"Scraping ended due to: {reason}")
            logger.info(f"Total reviews scraped: {self.total_reviews}")
        except Exception as e:
            logger.error(f"Error during closing: {e}")
