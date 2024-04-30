const express = require('express')

const app = express()
app.use(express.static('./build/site/'))

// app.use('/static/assets', express.static('./build/site/_'))

app.get('/', (req, res) => res.redirect('/apoc/4.4/'))

app.listen(8000, () => console.log('📘 http://localhost:8000/'))