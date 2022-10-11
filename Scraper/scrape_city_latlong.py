from bs4 import BeautifulSoup
import requests
import pandas as pd

site_to_scrape_link = "https://www.geonames.org/DE/largest-cities-in-germany.html"
html = requests.get(site_to_scrape_link)
soup = BeautifulSoup(html.content, 'html.parser')

coords = soup.find_all(name="a", rel="nofollow")
namen = soup.select("a[href*=https]")
names = namen[1:]

df = pd.DataFrame(columns=["city", "lat", "long"])


for i in range(len(names)):

    df = df.append({"city":names[i].getText(), "lat":float(coords[i].getText().split("/")[0]),"long":float(coords[i].getText().split("/")[1])}, ignore_index=True)

df.to_csv("cities.csv", index=True)
