import "./App.css";
import React, { useState, useEffect } from "react";
import Axios from "axios";

const currIP = "34.89.2.13";
//http://${currIP}:8081/api/getFromId/129477`
var MagicCards = [];
const foundMagicCards = [];

function App() {
  // Declare all state variables here
  // setMagicCards is used to save all cards that we got from the API
  const [magicCards, setMagicCards] = useState(MagicCards);
  // saves the state of the search field
  const [searchField, setSearchField] = useState("");
  // saves all the cards that are returned from the search
  const [searchedCards, setSearchedCards] = useState(foundMagicCards);

  function callMagicID(multiverseId) {
    Axios.get(`http://${currIP}:8081/api/getFromId/${multiverseId}`).then(
      (data) => {
        console.log(data.data);
        let fetchedData = data.data;
        let arrLen = fetchedData.length;
        for (let i = 0; i < arrLen; i++) {
          setMagicCards(MagicCards.push(fetchedData[i]));
        }
      }
    );
  }

  function callMagicName(name) {
    Axios.get(`http://${currIP}:8081/api/getFromName/${name}`).then((data) => {
      console.log(data.data);
      let fetchedData = data.data;
      let arrLen = fetchedData.length;
      for (let i = 0; i < arrLen; i++) {
        setMagicCards(MagicCards.push(fetchedData[i]));
      }
    });
  }

  // gets all magic cards (is limited for performance reasons)
  function getAllMagicCards() {
    Axios.get(`http://${currIP}:8081/api/get`).then((data) => {
      //console.log(data.data);
      let fetchedData = data.data;
      let arrLen = fetchedData.length;
      for (let i = 0; i < arrLen; i++) {
        setMagicCards(MagicCards.push(fetchedData[i]));
      }
      console.log(MagicCards);
    });
  }

  // make a call to the api to get all magic cards once on page load
  useEffect(() => {
    MagicCards = [];
    getAllMagicCards();
    setSearchedCards(MagicCards);
  }, []);

  const Cardfilter = (e) => {
    const search_val = e.target.value;
    setSearchField(search_val);
    MagicCards = [];
    // Check if the entered phrase is of type number
    if (search_val !== "" && !isNaN(search_val)) {
      callMagicID(search_val);
      setSearchedCards(MagicCards);
    }
    // Check if the entered phrase is of type string
    else if (search_val !== "" && isNaN(search_val)) {
      callMagicName(search_val);
      setSearchedCards(MagicCards);
    }

    // If the text field is empty, show all cards
    else {
      getAllMagicCards();
      setSearchedCards(MagicCards);
    }
  };

  return (
    <div>
      <div>
        <input
          type="search"
          value={searchField}
          onChange={Cardfilter}
          className="input"
          placeholder="Magic Card Name or ID"
        />
      </div>

      <div className="cards-list">
        {searchedCards && searchedCards.length > 0 ? (
          searchedCards.map((magicCard) => (
            <li key={magicCard.multiverseid} className="cards">
              <span className="magicCard-imageUrl">
                <img
                  className="magicCard-img"
                  alt="MTG card "
                  src={magicCard.imageUrl}
                />
              </span>
              <span className="cards-name">{magicCard.name}</span>
              <span className="magicCard-multiverseid">
                {magicCard.multiverseid}
              </span>
            </li>
          ))
        ) : (
          <p1>Couldn't find any magic cards.</p1>
        )}
      </div>
    </div>
  );
}

export default App;
