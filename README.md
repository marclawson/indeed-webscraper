# Indeed Web Scraper

A Python web scraper for extracting job listings from Indeed.co.uk, specifically targeting data science and related positions.

## Features

- Scrapes job listings for various data science roles
- Filters jobs with salary information
- Stores data in a database using the WebScraperDB class
- Sends progress notifications via Slack webhooks
- Includes retry logic for robustness

## Setup

1. Clone this repository
2. Install required dependencies:
   ```bash
   pip install requests beautifulsoup4 pandas numpy tqdm
   ```
3. Copy the configuration template:
   ```bash
   cp config.example.py config.py
   ```
4. Edit `config.py` and add your Slack webhook URL
5. Ensure you have a database configuration file (`connection.cfg`) for the WebScraperDB class

## Usage

Run the scraper:
```bash
python indeed_webscraper.py
```

The scraper will:
- Search for jobs in the following categories:
  - Data Scientist
  - Data Analyst
  - BI Analyst
  - NLP Engineer
  - NLP Scientist
  - Business Intelligence
  - Machine Learning
  - Research Scientist
- Filter for jobs with salary ≥ £20,000
- Send progress updates to Slack
- Store results in the database

## Configuration

The `config.py` file contains sensitive configuration values:
- `SLACK_HOOK_URL`: Your Slack webhook URL for notifications

**Important**: Never commit `config.py` to version control as it contains sensitive information.

## Files

- `indeed_webscraper.py`: Main scraper script
- `WebScraperDB.py`: Database handling class
- `config.py`: Configuration file (not tracked by git)
- `config.example.py`: Configuration template
- `.gitignore`: Ensures sensitive files aren't tracked

## License

Please ensure you comply with Indeed's terms of service when using this scraper.
