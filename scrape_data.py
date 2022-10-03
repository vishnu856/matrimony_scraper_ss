from typing import Dict, List
import bs4
import pandas as pd
import asyncio
import click
import requests

WAIT_S = 0.1
OUTPUT_PATH = "./result.csv"


class MatrimonyScraper:
    def __init__(self):
        """Initialize matrimony scraper"""
    
    def get_name_from_html(self, curl_output: str) -> str:
        """Parse out name from given HTML"""
        soup = bs4.BeautifulSoup(curl_output, features="lxml")
        return self._get_name_from_soup(soup)
    
    def _get_name_from_soup(self, soup: bs4.BeautifulSoup) -> str:
        """Get name from bs4 soup"""
        all_spans = soup.find_all("span")
        assert len(all_spans) > 0
        return self._postprocess_name(all_spans[5])

    def _postprocess_name(self, name_span: bs4.element.Tag) -> str:
        """Get name from bs4 tag for span"""
        name_str = str(name_span.contents[0]).strip().replace("\xa0", "").replace("\n", "").replace(".", " ")
        name_str = " ".join(name_str.split())
        return name_str

    async def run(self, urls: List[str]) -> pd.DataFrame:
        person_details: List[Dict[str, str]] = []
        for url in urls:
            # url = "https://www.ssmatri.com/ssnmlprofile.php?id=46398&msearch=yes"
            r = requests.get(url)
            if r.status_code != 200:
                print(f"Request failed to {url}")
                continue
            name = self.get_name_from_html(r.text)
            person_details.append({
                "name": name,
                "url": url
            })

            await asyncio.sleep(WAIT_S)

        df = pd.DataFrame(person_details)
        df.to_csv(OUTPUT_PATH, columns=["name", "url"])
        return df

@click.command()
@click.option("--path_to_urls", "-p", required=True, type=click.Path(), help="Path to URLs of profiles")
def parse_matrimony(path_to_urls):
    urls: List[str] = []
    with open(path_to_urls) as file:
        while (line := file.readline().rstrip()):
            urls.append(line)

    scraper = MatrimonyScraper()
    asyncio.get_event_loop().run_until_complete(scraper.run(urls))


if __name__ == "__main__":
    parse_matrimony()
