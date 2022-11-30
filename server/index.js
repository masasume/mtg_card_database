const express = require("express");
const db = require("./db");
const cors = require("cors");

const app = express();
// this is the port where express can be accessed
const PORT = 8081;
app.use(cors());
app.use(express.json());

// Route to get all cards
app.get("/api/get", (req, res) => {
  db.query("SELECT * FROM user_magic_cards limit 25;", (err, result) => {
    if (err) {
      console.log(err);
    }
    res.send(result);
  });
});

// Route to get all cards with specific id
app.get("/api/getFromId/:id", (req, res) => {
  const id = req.params.id;
  db.query(
    "SELECT * FROM user_magic_cards WHERE multiverseid = ?;",
    id,
    (err, result) => {
      if (err) {
        console.log(err);
      }
      res.send(result);
    }
  );
});

// Route to get all cards with specific name
app.get("/api/getFromName/:name", (req, res) => {
  const name = req.params.name;
  db.query(
    "SELECT * FROM user_magic_cards WHERE name like ?;",
    name,
    (err, result) => {
      if (err) {
        console.log(String(db.query));
        console.log(err);
      }
      res.send(result);
    }
  );
});

app.listen(PORT, () => {
  console.log(`Server is running on ${PORT}`);
});
