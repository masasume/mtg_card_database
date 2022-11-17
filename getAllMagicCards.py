import requests
import json
from datetime import datetime


def getAllMTGCards():
    response = requests.get(
        "https://api.magicthegathering.io/v1/cards?pageSize=100&page=1"
    )
    totalCount = response.headers["Total-Count"]
    totalCount = int((int(totalCount) / 100))
    cards = "{'cards': "
    print(cards)
    cards = cards + str(response.json()["cards"])
    for i in range(2, 3):
        print(str(i) + "von" + str(totalCount))
        response = requests.get(    
            "https://api.magicthegathering.io/v1/cards?pageSize=100&page=" + str(i)
        )
        cards = cards + response.json()["cards"]
    cards = cards + "}"
    cardsJson = json.dumps(cards)
    yearMonthDay = datetime.now().strftime('%Y-%m-%d')
    text_file = open("mtgcards_"+yearMonthDay+".json", "w")
    text_file.write(cardsJson)
    return

getAllMTGCards()