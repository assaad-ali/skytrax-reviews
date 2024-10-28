import scrapy
from loguru import logger

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
        super().__init__(*args, **kwargs)
        self.total_reviews = 0
        logger.info("BritishAirwaysSpider initialized.")

    def parse(self, response):
        logger.info(f"Parsing page: {response.url}")
        reviews = response.css('article.comp_media-review-rated')
        review_count = len(reviews)
        logger.info(f"Found {review_count} reviews on this page.")

        for idx, review in enumerate(reviews, start=1):
            review_data = {
                'title': review.css('h2.text_header::text').get(default='').strip(),
                'author': review.css('span[itemprop="name"]::text').get(default='').strip(),
                'date': review.css('meta[itemprop="datePublished"]::attr(content)').get(default='').strip(),
                'content': review.css('div[itemprop="reviewBody"]::text').get(default='').strip(),
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

        # Follow pagination
        next_page = response.css('li.pagination-next a::attr(href)').get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            logger.info(f"Following to next page: {next_page_url}")
            yield scrapy.Request(next_page_url, callback=self.parse)
        else:
            logger.info("No more pages to scrape.")

    def close(self, reason):
        logger.info(f"Scraping ended due to: {reason}")
        logger.info(f"Total reviews scraped: {self.total_reviews}")
