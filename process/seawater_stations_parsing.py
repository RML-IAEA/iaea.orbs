import re
import pdfplumber
import pandas as pd


text = ""
with pdfplumber.open("stations/R6zahyo.pdf") as pdf_file:
    for page in pdf_file.pages:
        text += page.extract_text()

def extract_station_lines(txt):
    """
    Parsing Seawater Stations PDF
    """
    txt = txt.replace("※１", "")
    lines = txt.split("\n")
    keywords_to_delete = ["Monitoring", "F-P36", "point", "Longitude", "Latitude", "離", "地"]
    pattern = re.compile("|".join(keywords_to_delete))
    filtered_lines = [line for line in lines if not pattern.search(line)]
    return filtered_lines

data = extract_station_lines(text)
df = pd.DataFrame([x.split() for x in data], columns=["station", "lon", "lat"])
# df['Organization'] = df['Station'].apply(lambda x:
#     "Fukushima Prefecture" if x.startswith('F')
#     else "Ministry of the Environment" if x.startswith('E')
#     else "Ministry of Land, Infrastructure, Transport and Tourism" if x.startswith('KK-U1')
#     else "Nuclear Regulation Authority" if x.startswith(('M', 'K', 'C'))
#     else "TEPCO" if x.startswith('T')
#     else "Local governments" if x.startswith('TBD')
#     else "Unknown"
# )

df['org'] = df['station'].apply(lambda x:
    "Fukushima Prefecture" if x.startswith('F')
    else "MOE" if x.startswith('E')
    else "MLITT" if x.startswith('KK-U1')
    else "NRA" if x.startswith(('M', 'K', 'C'))
    else "TEPCO" if x.startswith('T')
    else "Local governments" if x.startswith('TBD')
    else "Unknown"
)

cols = df.columns.tolist()
cols = cols[-1:] + cols[:-1]
df = df[cols]
df.to_csv("stations/Seawater_station_points.csv", index=False)
