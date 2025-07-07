from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
import time
import io
import pickle

path = 'https://ardupilot.org/plane/docs/logmessages.html'
driver = webdriver.Chrome()
driver.get(path)
time.sleep(2)

parent_section = driver.find_element(By.ID, 'onboard-message-log-messages')
sections = parent_section.find_elements(By.XPATH, './*[@id]')

tables_dict = {}

for section in sections[1:]:
    section_id = section.get_attribute('id')
    table_div = section.find_element(By.XPATH, './/div//table')
    html = table_div.get_attribute('outerHTML')
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    df = pd.read_html(io.StringIO(str(table)))[0].astype(str)
    tables_dict[section_id.upper()] = df

with open("documentation.pkl", "wb") as f:
    pickle.dump(tables_dict, f)