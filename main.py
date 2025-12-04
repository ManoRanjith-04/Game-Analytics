import requests
import mysql.connector
import pandas as pd

# ---------------------------------------------------
# 1. FETCH DATA FROM SPORTRADAR API
# ---------------------------------------------------

API_KEY = "fSOrFSr27dmtDjEXaztVigFWdfK8y3dMwIsMaEXW"  

url = "https://api.sportradar.com/tennis/trial/v3/en/competitions.json"
headers = {
    "accept": "application/json",
    "x-api-key": API_KEY
}

print("Fetching data from Sportradar...")
response = requests.get(url, headers=headers)
data = response.json()
print("Data received successfully.")

# 2. PARSE COMPETITIONS + CATEGORIES
# ---------------------------------------------------

categories = {}
competitions = []

if "competitions" in data:
    for comp in data["competitions"]:
        # Extract category info
        cat = comp.get("category", {})
        cat_id = cat.get("id")
        cat_name = cat.get("name")

        if cat_id:
            categories[cat_id] = cat_name

        # Extract competition info
        competitions.append({
            "competition_id": comp.get("id"),
            "competition_name": comp.get("name"),
            "parent_id": comp.get("parent_id"),
            "type": comp.get("type"),
            "gender": comp.get("gender"),
            "category_id": cat_id
        })

print(f"Parsed {len(categories)} categories and {len(competitions)} competitions.")

# 3. CONNECT TO MYSQL WORKBENCH
# ---------------------------------------------------

conn = mysql.connector.connect(
    host="127.0.0.1",        
    user="root",        
    password="Mano1426$",
    database="sportradar" 
)

cursor = conn.cursor()

print("Connected to MySQL.")

# 4. CREATE TABLES
# ---------------------------------------------------

cursor.execute("DROP TABLE IF EXISTS competitions;")
cursor.execute("DROP TABLE IF EXISTS categories;")

create_categories = """
CREATE TABLE categories (
    category_id VARCHAR(50) PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
);
"""

create_competitions = """
CREATE TABLE competitions (
    competition_id VARCHAR(50) PRIMARY KEY,
    competition_name VARCHAR(100) NOT NULL,
    parent_id VARCHAR(50),
    type VARCHAR(20) NOT NULL,
    gender VARCHAR(10) NOT NULL,
    category_id VARCHAR(50),
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
);
"""

cursor.execute(create_categories)
cursor.execute(create_competitions)
conn.commit()

print("Tables created successfully.")

# 5. INSERT DATA
# ---------------------------------------------------

# Insert categories
for cat_id, cat_name in categories.items():
    cursor.execute(
       "INSERT IGNORE INTO categories (category_id, category_name) VALUES (%s, %s)",
       (cat_id, cat_name)
    )

# Insert competitions
for comp in competitions:
    gender = comp["gender"] if comp["gender"] else "unknown"
    ctype = comp["type"] if comp["type"] else "unknown"

    cursor.execute("""
    INSERT IGNORE INTO competitions (
        competition_id, competition_name, parent_id, type, gender, category_id
    ) VALUES (%s, %s, %s, %s, %s, %s)
""", (
    comp["competition_id"],
    comp["competition_name"],
    comp["parent_id"],
    ctype,
    gender,
    comp["category_id"]
))


conn.commit()
print("Data inserted successfully.")

# 6. RUN SQL ANALYSIS QUERIES
# ---------------------------------------------------

def run_query(title, query):
    print("\n========== " + title + " ==========")
    df = pd.read_sql(query, conn)
    print(df)
    return df

# 1. List all competitions with category name
run_query("1. Competitions with Category",
"""
SELECT c.competition_id, c.competition_name, c.type, c.gender,
       cat.category_name
FROM competitions c
LEFT JOIN categories cat ON c.category_id = cat.category_id;
"""
)

# 2. Number of competitions in each category
run_query("2. Competitions per Category",
"""
SELECT cat.category_name, COUNT(*) AS competition_count
FROM competitions c
JOIN categories cat ON c.category_id = cat.category_id
GROUP BY cat.category_name;
"""
)

# 3. Competitions of type 'doubles'
run_query("3. Doubles Competitions",
"""
SELECT competition_id, competition_name
FROM competitions
WHERE LOWER(type)='doubles';
"""
)

# 4. Get competitions for specific category
target_category = "ITF Men"
run_query(f"4. Competitions in Category: {target_category}",
f"""
SELECT c.competition_id, c.competition_name
FROM competitions c
JOIN categories cat ON c.category_id = cat.category_id
WHERE cat.category_name = '{target_category}';
"""
)

# 5. Parent competitions + sub-competitions
run_query("5. Parent & Child Competitions",
"""
SELECT p.competition_name AS parent,
       c.competition_name AS child
FROM competitions c
LEFT JOIN competitions p ON c.parent_id = p.competition_id
WHERE c.parent_id IS NOT NULL;
"""
)

# 6. Distribution of competition types by category
run_query("6. Competition Type Distribution",
"""
SELECT cat.category_name, c.type, COUNT(*) AS count
FROM competitions c
JOIN categories cat ON c.category_id = cat.category_id
GROUP BY cat.category_name, c.type;
"""
)

# 7. Competitions with no parent
run_query("7. Top-Level Competitions",
"""
SELECT competition_id, competition_name
FROM competitions
WHERE parent_id IS NULL;
"""
)

# 1. FETCH DATA FROM API
# ---------------------------------------------------

url = "https://api.sportradar.com/tennis/trial/v3/en/complexes.json"
headers = {
    "accept": "application/json",
    "x-api-key": "fSOrFSr27dmtDjEXaztVigFWdfK8y3dMwIsMaEXW"
}

response = requests.get(url, headers=headers)
data = response.json()

# Parse complexes
complexes = {}
venues = []

for comp in data.get("complexes", []):
    comp_id = comp.get("id")
    comp_name = comp.get("name")

    complexes[comp_id] = comp_name

    # Venues inside complex
    for venue in comp.get("venues", []):
        venues.append({
            "venue_id": venue.get("id"),
            "venue_name": venue.get("name"),
            "city_name": venue.get("city", ""),
            "country_name": venue.get("country", {}).get("name", ""),
            "country_code": venue.get("country", {}).get("code", ""),
            "timezone": venue.get("timezone", ""),
            "complex_id": comp_id
        })


# 2. CONNECT TO MYSQL WORKBENCH
# ---------------------------------------------------

conn = mysql.connector.connect(
    host="127.0.0.1",        # <--- change
    user="root",        # <--- change
    password="Mano1426$",# <--- change
    database="sportradar" # <--- change
)

cursor = conn.cursor()

print("Connected to MySQL.")

# 3. CREATE TABLES
# ---------------------------------------------------

cursor.execute("""
CREATE TABLE IF NOT EXISTS complexes (
    complex_id VARCHAR(50) PRIMARY KEY,
    complex_name VARCHAR(100) NOT NULL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS venues (
    venue_id VARCHAR(50) PRIMARY KEY,
    venue_name VARCHAR(100) NOT NULL,
    city_name VARCHAR(100) NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    country_code CHAR(3) NOT NULL,
    timezone VARCHAR(100) NOT NULL,
    complex_id VARCHAR(50),
    FOREIGN KEY (complex_id) REFERENCES complexes(complex_id)
)
""")

conn.commit()
print("Tables created.")

# 4. INSERT DATA SAFELY
# ---------------------------------------------------

# Insert complexes
for comp_id, comp_name in complexes.items():
    cursor.execute("""
        INSERT IGNORE INTO complexes (complex_id, complex_name)
        VALUES (%s, %s)
    """, (comp_id, comp_name))

# Insert venues
for v in venues:
    cursor.execute("""
        INSERT IGNORE INTO venues (
            venue_id, venue_name, city_name, country_name,
            country_code, timezone, complex_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        v["venue_id"], v["venue_name"], v["city_name"],
        v["country_name"], v["country_code"], v["timezone"],
        v["complex_id"]
    ))

conn.commit()
print("Data inserted successfully.")

# Run SQL Queries

def run_query(sql):
    return pd.read_sql(sql, conn)


print("\n--- 1. List all venues with complex name ---")
print(run_query("""
SELECT v.venue_name, c.complex_name
FROM venues v
JOIN complexes c ON v.complex_id = c.complex_id
"""))

# 2. Count venues per complex
print("\n--- 2. Count venues per complex ---")
print(run_query("""
SELECT c.complex_name, COUNT(v.venue_id) AS venue_count
FROM complexes c
LEFT JOIN venues v ON c.complex_id = v.complex_id
GROUP BY c.complex_id
"""))

# 3. Venues in specific country (e.g., Chile)
print("\n--- 3. Venues in specific country (e.g., Chile) ---")
print(run_query("""
SELECT * FROM venues
WHERE country_name = 'Chile'
"""))

# 4. All venues and their timezones
print("\n--- 4. All venues and their timezones ---")
print(run_query("""
SELECT venue_name, timezone FROM venues
"""))


# 5. Complexes with more than 1 venue
print("\n--- 5. Complexes with more than 1 venue ---")
print(run_query("""
SELECT c.complex_name, COUNT(v.venue_id) AS venue_count
FROM complexes c
JOIN venues v ON c.complex_id = v.complex_id
GROUP BY c.complex_id
HAVING COUNT(v.venue_id) > 1
"""))

# 6. Venues grouped by country
print("\n--- 6. Venues grouped by country ---")
print(run_query("""
SELECT country_name, COUNT(*) AS venue_count
FROM venues
GROUP BY country_name
"""))

# 7. All venues for a specific complex

print("\n--- 7. All venues for a specific complex (e.g., Nacional) ---")
print(run_query("""
SELECT v.*
FROM venues v
JOIN complexes c ON v.complex_id = c.complex_id
WHERE c.complex_name LIKE '%Nacional%'
"""))

#THE DOUBLES COMPETITOR RANKINGS DATA

# 1. FETCH DATA FROM API
# ---------------------------------------------------

# API endpoint and key
url = "https://api.sportradar.com/tennis/trial/v3/en/double_competitors_rankings.json"
headers = {
    "accept": "application/json",
    "x-api-key": "fSOrFSr27dmtDjEXaztVigFWdfK8y3dMwIsMaEXW"
}

response = requests.get(url, headers=headers)
data = response.json() 

#2: Extract competitors and rankings
{
  "competitors": [
    {
      "id": "abc123",
      "name": "Player A",
      "country": "Croatia",
      "country_code": "CRO",
      "abbreviation": "PLA",
      "rankings": {
          "rank": 1,
          "points": 1200,
          "movement": 0,
          "competitions_played": 10
      }
    },
    ...
  ]
}


# normalize it to two DataFrames:
competitors_list = []
rankings_list = []

for comp in data['doubles_rankings']:
    competitor_info = comp['competitor']
    # Competitors table
    competitors_list.append({
        "competitor_id": competitor_info['id'],
        "name": competitor_info['name'],
        "country": competitor_info['country'],
        "country_code": competitor_info['country_code'],
        "abbreviation": competitor_info['abbreviation']
    })
    # Ranking table
    rankings_list.append({
        "rank": comp['rank'],
        "points": comp['points'],
        "movement": comp['movement'],
        "competitions_played": comp['competitions_played'],
        "competitor_id": competitor_info['id']
    })

df_competitors = pd.DataFrame(competitors_list)
df_rankings = pd.DataFrame(rankings_list)


# 3. CONNECT TO MYSQL WORKBENCH
# ---------------------------------------------------

conn = mysql.connector.connect(
    host="127.0.0.1",     
    user="root",        
    password="Mano1426$",
    database="sportradar" 
)

cursor = conn.cursor()

print("Connected to MySQL.")

# 3. CREATE TABLES 
# ---------------------------------------------------

cursor.execute("""
CREATE TABLE IF NOT EXISTS competitors (
    competitor_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    country_code CHAR(3) NOT NULL,
    abbreviation VARCHAR(10) NOT NULL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS competitor_rankings (
    rank_id INT AUTO_INCREMENT PRIMARY KEY,
    `rank` INT NOT NULL,
    movement INT NOT NULL,
    points INT NOT NULL,
    competitions_played INT NOT NULL,
    competitor_id VARCHAR(50),
    FOREIGN KEY (competitor_id) REFERENCES competitors(competitor_id)
)
""")

conn.commit()
print("Tables created successfully.")

# 4. INSERT DATA 
# ---------------------------------------------------

# Insert competitors
for comp in competitors:
    cursor.execute("""
        INSERT IGNORE INTO competitors (
            competitor_id, name, country, country_code, abbreviation
        ) VALUES (%s, %s, %s, %s, %s)
    """, (
        comp["competitor_id"], comp["name"], comp["country"],
        comp["country_code"], comp["abbreviation"]
    ))

# Insert rankings
# Insert rankings
for r in rankings:
    rank_value = r["rank"] if r["rank"] is not None else 0
    
    cursor.execute("""
        INSERT INTO competitor_rankings (
            `rank`, movement, points, competitions_played, competitor_id
        ) VALUES (%s, %s, %s, %s, %s)
    """, (
        rank_value, r["movement"], r["points"],
        r["competitions_played"], r["competitor_id"]
    ))

conn.commit()
print("Data inserted successfully.")

def table_exists(table_name):
    """Check if a table exists in the current database."""
    query = f"""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = DATABASE() AND table_name = '{table_name}'
    """
    return pd.read_sql(query, conn).iloc[0, 0] == 1

def run_query(sql):
    """Run SQL query and return a DataFrame."""
    return pd.read_sql(sql, conn)

# Check if Competitor_Rankings table exists
if table_exists("Competitor_Rankings"):
    print("\n--- All competitors with rank & points ---")
    sql = """
    SELECT c.name, c.country, c.country_code, c.abbreviation,
           r.rank, r.points, r.competitions_played, r.movement
    FROM Competitors c
    LEFT JOIN Competitor_Rankings r ON c.competitor_id = r.competitor_id
    ORDER BY r.rank
    """
else:
    print("\n--- All competitors (Rankings table not found) ---")
    sql = """
    SELECT competitor_id, name, country, country_code, abbreviation
    FROM Competitors
    ORDER BY name
    """

df = run_query(sql)
print(df)
print("\n--- 2. Competitors ranked in the TOP 5 ---")
print(run_query("""
SELECT c.name, r.rank, r.points
FROM competitors c
JOIN competitor_rankings r ON c.competitor_id = r.competitor_id
WHERE r.rank <= 5
ORDER BY r.rank
"""))


print("\n--- 3. Competitors with no rank movement ---")
print(run_query("""
SELECT c.name, r.rank, r.movement
FROM competitors c
JOIN competitor_rankings r ON c.competitor_id = r.competitor_id
WHERE r.movement = 0
"""))


print("\n--- 4. Total points of competitors from Croatia ---")
print(run_query("""
SELECT SUM(r.points) AS total_points
FROM competitors c
JOIN competitor_rankings r ON c.competitor_id = r.competitor_id
WHERE c.country = 'Croatia'
"""))

print("\n--- 5. Count competitors per country ---")
print(run_query("""
SELECT country, COUNT(*) AS competitor_count
FROM competitors
GROUP BY country
ORDER BY competitor_count DESC
"""))

print("\n--- 6. Competitors with highest points (Top Performers) ---")
print(run_query("""
SELECT c.name, r.points
FROM competitors c
JOIN competitor_rankings r ON c.competitor_id = r.competitor_id
ORDER BY r.points DESC
LIMIT 5
"""))
