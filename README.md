# Sneakers Web Scraper

## Project Description

A web scraper to analyze and determine the most sold men's sneakers from Zappos.com, featuring:

- Asynchronous web scraping
- Review sentiment analysis
- Database storage
- Excel export of results

## Prerequisites

- Python 3.12
- Poetry

## Installation

1. Clone the repository:

```bash
git clone https://github.com/inth3wild/sneakers-scraper.git
cd sneakers-scraper
```

2. Install dependencies using Poetry:

```bash
poetry install
```

## Configuration

Ensure you have Poetry installed:

```bash
pip install poetry
```

## Usage

Run the scraper:

```bash
poetry run python scraper.py
```

## Output

The script generates:

- `zappos_sneakers.xlsx` with three sheets:

  1. Sneakers: Raw scraping data
  2. Analysis: Review sentiment details
  3. Most Sold: Top 20 sneakers by positive reviews

- `zappos_sneakers.db`: SQLite database with scraped data

## Dependencies

- aiohttp
- beautifulsoup4
- textblob
- sqlalchemy
- xlsxwriter

## Notes

- Respects Zappos' terms of service
- Implements rate limiting
- Provides detailed logging

## Directory structure

```
inth3wild-sneakers-scraper/
    ├── pyproject.toml
    ├── review-snippet.html
    ├── sample-listing-page.html
    ├── sample-reviews-page.html
    └── scraper.py
```
