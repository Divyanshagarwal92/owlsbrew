var express = require('express');
var app = express();

app.use(express.static('./public'));

var routes = require('./routes/index');
app.use('/', routes);

app.listen(3000);