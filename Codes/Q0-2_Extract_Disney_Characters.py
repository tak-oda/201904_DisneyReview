import urllib.request
from bs4 import BeautifulSoup

url1 = "https://www.listchallenges.com/the-ultimate-list-of-disney-characters"
url2 = "https://www.listchallenges.com/the-ultimate-list-of-disney-characters/checklist/2"
url3 = "https://www.listchallenges.com/the-ultimate-list-of-disney-characters/checklist/3"
url4 = "https://www.listchallenges.com/the-ultimate-list-of-disney-characters/checklist/4"
url5 = "https://www.listchallenges.com/the-ultimate-list-of-disney-characters/checklist/5"

url = [ url1, url2, url3, url4, url5]

pattern = ""

for page in url:
    
    soup = BeautifulSoup(urllib.request.urlopen(page).read())
    
    div = soup.find_all("div")
    
    
    for tag in div:
        try:
            string_ = tag.get("class").pop(0)
            
            if string_ in "item-name":
                pattern = pattern + "|" + tag.string.strip().lower()
                
                
        except:
            pass

print(pattern)
