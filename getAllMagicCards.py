import requests
import json

def getAllMTGCards():
    response = requests.get(
        "https://api.magicthegathering.io/v1/cards?pageSize=100&page=1"
    )
    totalCount = response.headers["Total-Count"]
    totalCount = int((int(totalCount) / 100))
    cards = response.json()["cards"]
    for i in range(2, 5):
        print(str(i) + "von" + str(totalCount))
        response = requests.get(    
            "https://api.magicthegathering.io/v1/cards?pageSize=100&page=" + str(i)
        )
        cards = cards + response.json()["cards"]
    cardsJson = json.dumps(cards)
    text_file = open("mtgcards.json", "w")
    text_file.write(cardsJson)
    return

getAllMTGCards()