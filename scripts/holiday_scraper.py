import requests
from bs4 import BeautifulSoup
import pandas as pd

def date_to_num(month_abbr):
    month_map = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
        "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
        "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
    }
    return month_map.get(month_abbr.capitalize(), None)

def scrape_bangkok_holidays(start_year=2021, end_year=2025):
    holidays = []

    for year in range(start_year, end_year + 1):
        url = f'https://www.officeholidays.com/countries/thailand/bangkok/{year}'
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to retrieve data for {year}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')
        if not table:
            print(f"No table found for {year}")
            continue

        rows = table.find_all('tr')[1:]  # Skip header
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 4:
                date = cols[1].get_text(strip=True).split(" ")
                name = cols[2].get_text(strip=True)
                date_str = f"{year}-{date_to_num(date[0])}-{date[1]}"
                holidays.append({
                    'holiday_date': date_str,
                    'holiday_name': name
                })

    return pd.DataFrame(holidays)

# Save result
holiday_df = scrape_bangkok_holidays()
holiday_df.to_csv('bangkok_holidays.csv', index=False)
print("✅ Holidays saved")