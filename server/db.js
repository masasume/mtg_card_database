const mysql = require('mysql2')
const db = mysql.createConnection({
    host: "172.17.0.2",
    user: "root",
    password: "MagicPassword",
    database:"MagicTheGathering" 
})

module.exports = db;