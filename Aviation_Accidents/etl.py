#!/usr/bin/env python
# coding: utf-8

# # Data Extraction 

# ## Web Scraping

# In[142]:


import re
import time
from typing import List, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup


# In[143]:


BASE = "https://aviation-safety.net/database/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


# In[144]:


def get_html(url: str, session: requests.Session, timeout: int = 30) -> str:
    r = session.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


# In[49]:


def get_max_page(year: int, session: requests.Session) -> int:
    """
    Finds the highest page number from pagination links like /database/year/2000/7
    Default: 1 if no pagination found
    """
    url = f"https://aviation-safety.net/database/year/{year}/1"
    html = get_html(url, session)
    soup = BeautifulSoup(html, "html.parser")

    pages = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        m = re.search(rf"/database/year/{year}/(\d+)", href)
        if m:
            pages.add(int(m.group(1)))

    return max(pages) if pages else 1


# In[50]:


def pick_accident_table(tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    From all tables on the page, pick the one that looks like the accident list:
    columns usually include: acc. date, type, reg., operator, fat., location, dmg
    """
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        # heuristics: must have acc & date + type + location (or operator)
        if any("acc" in c and "date" in c for c in cols) and ("type" in cols) and ("location" in cols):
            return t
    return None


# In[51]:


import pandas as pd

def scrape_table(url: str, session: requests.Session) -> Optional[pd.DataFrame]:
    html = get_html(url, session)
    tables = pd.read_html(html)

    # διάλεξε τον σωστό πίνακα: αυτόν που έχει header σαν "acc. date" ή "type" κλπ
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if any("acc" in c and "date" in c for c in cols) or "type" in cols or "operator" in cols:
            return t

   
    return None


# In[60]:


def scrape_page(year: int, page: int, session: requests.Session) -> Optional[pd.DataFrame]:
    url = f"https://aviation-safety.net/database/year/{year}/{page}"
    html = get_html(url, session)

    # read all tables then pick the correct one
    tables = pd.read_html(html)
    t = pick_accident_table(tables)
    if t is None or t.empty:
        return None
        # normalize + rename columns (case/spacing tolerant)
    t.columns = [str(c).strip().lower() for c in t.columns]

    # the site uses "acc. date" (with space), sometimes variations; normalize
    rename_map = {
        "acc. date": "Accident_Date",
        "acc. date ": "Accident_Date",
        "type": "Type",
        "reg.": "Registration",
        "operator": "Operator",
        "fat.": "Fatalities",
        "location": "Location",
        "dmg": "Aircraft_Damage",
    }
    t = t.rename(columns=rename_map)

    wanted = ["Accident_Date", "Type", "Registration", "Operator", "Fatalities", "Location", "Aircraft_Damage"]
    keep = [c for c in wanted if c in t.columns]
    t = t[keep].copy()

    t["Year"] = year
    t["Page"] = page
    t["Source_Url"] = url

    # fat to numeric where possible
    if "Fatalities" in t.columns:
        t["Fatalities"] = pd.to_numeric(
            t["Fatalities"].astype(str).str.replace(r"[^\d]", "", regex=True),
            errors="coerce"
        )

    return t

def scrape_years(year_start: int = 2000, year_end: int = 2025, out_csv: str = "asn_accidents.csv", polite_sleep: float = 0.4) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []

    with requests.Session() as session:
        for year in range(year_start, year_end + 1):
            max_page = get_max_page(year, session)
            print(f"{year}: {max_page} pages")

            for page in range(1, max_page + 1):
                df = scrape_page(year, page, session)
                if df is not None and not df.empty:
                    frames.append(df)
                    print(f"  page {page}: {len(df)} rows")
                else:
                    print(f"  page {page}: [WARN] no table found")
                time.sleep(polite_sleep)

    if not frames:
        raise RuntimeError("No data scraped. The site may be blocking requests or the structure changed.")

    data = pd.concat(frames, ignore_index=True)
    data.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"Saved {out_csv} | rows={len(data):,} | cols={len(data.columns)}")
    return data




# In[145]:


df = scrape_years(
    year_start=2000,
    year_end=2025,
    out_csv="asn_2000.csv",
    polite_sleep=0.5
)
df.head()


# # Transformation Phase

# ## Data Processing

# ## 1. Understanding Variable Types

# The dataset consists of 6860 observations and 10 variables in order to describe aviation accident during 25 years.<br> The variables include categorical and numerical information.

# In[66]:


df.info()


# ## 2. Clean, Validate and Transform Data

# Subsequently, after identifying the types of variables, it was considered necessary to apply appropriate data handling techniques. This included the transformation of categorical variables into numerical ones, as well as the treatment of missing and identification of  inconsistent values discrepancies and in order to ensure and improve data consistency, interpretability, and suitability for subsequent analysis.
# 
# Various approaches for handling missing data have been proposed in the literature. The most commonly used methods are presented below:
# <ul>
# <li> <u>Removal and deletion of values: </u> <br>
# This approach carries the risk of losing a significant amount of information. Therefore, it is advisable to first examine the proportion of missing values within a given feature. If this proportion exceeds an acceptable threshold, the entire feature may be removed from the dataset.
# 
# <li> <u> Value imputation: </u> <br>
# Missing values are commonly replaced using statistical measures such as the median or the mean of the available data.
# </ul>
# 
# Ultimately, the approach adopted in this study is the removal and deletion of such values.

# In[77]:


df["Aircraft_Damage"].unique()


# In[146]:


df["Aircraft_Damage"] = df["Aircraft_Damage"].replace(
{'sub':"Substantial",
'w/o':"Destroyed, written off",
'non':"None",
'min':"Minor, repaired",
'mis' : 'NULL',
'unk' :"Unknown"
})



# In[147]:


df["Accident_Date"] = pd.to_datetime(df["Accident_Date"], errors="coerce")
df[df["Accident_Date"].isna()]


# In[148]:


df[df["Accident_Date"].dt.year != df["Year"]]


# In[149]:


df["Type"].nunique()


# In[150]:


df.describe()


# In[151]:


df["Fatalities"] = df["Fatalities"].astype(str)
df["Fatalities"].value_counts().head(20)


# In[154]:


df.isna().sum()


# In[153]:


df=df.dropna()


# In[191]:


df.head()


# # Data Loading 
# 

# # Append to a PostgreSQL table

# In[244]:


from sqlalchemy import create_engine
from sqlalchemy import text

engine = create_engine(
"postgresql+psycopg2://postgres:postgres@db:5433/aviation_db"
)

con = engine.connect()

sql = """ 
    DROP TABLE IF EXISTS plane

    """

## create an empty table tennis 
sql = """
 CREATE TABLE IF NOT EXISTS plane (
 accident_date VARCHAR(50),
 type TEXT,
 registration TEXT,
 operator TEXT,
 fatalities INT,
 location TEXT,
 aircraft_damage TEXT,
 year INT,
 page INT,
 source_url TEXT)
;

""" 



# execute the 'sql' query
with engine.connect().execution_options(autocommit=True) as conn:
    query = conn.execute(text(sql))



df["Fatalities"] = pd.to_numeric(df["Fatalities"], errors="coerce").astype("Int64")


# In[245]:


df2 = df.rename(columns={
    "Accident_Date": "accident_date",
    "Type": "type",
    "Registration": "registration",
    "Operator": "operator",
    "Fatalities": "fatalities",
    "Aircraft_Damage": "aircraft_damage",
    "Location": "location",
    "Year": "year",
    "Page": "page",
    "Source_Url": "source_url",
})


#df["fatalities"] = pd.to_numeric(df["fatalities"], errors="coerce").astype("Int64")


# In[129]:


#!pip install psycopg2-binary sqlalchemy


# In[246]:


with engine.connect().execution_options(autocommit=True) as conn:
    df2.to_sql("plane", engine, if_exists="append", index=False)


# In[248]:


pd.read_sql("SELECT * FROM plane;", engine)


# In[224]:


sql = """ select * FROM plane """

with engine.connect().execution_options(autocommit=True) as conn:
    query = conn.execute(text(sql))
    rows = query.mappings().all()
    print(rows)
    


# In[226]:


df2.head()


# In[219]:


df2.info()


# In[ ]:



