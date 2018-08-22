---
title: API Reference

language_tabs: # must be one of https://git.io/vQNgJ
  - Examples

toc_footers:
  - <a href='#'><i>Sistemas do Futuro</i></a>

includes:
  - errors

search: true
---
  
# Introduction

Welcome to the In patrimonium Premium API! You can use our API to access In patrimonium Premium API endpoints, which can get information of different categories in our database.

This API documentation page was created with [Slate](https://github.com/lord/slate) and it belongs to 'Sistemas do Futuro'.


# In Patrimonium Methods

## URL

> This method returns a URL like the following one:

```
https://museudigital.marinha.pt/pesquisa/ficha.aspx?t=o&id=43
```

This endpoint retrieves a specific URL from museudigital.marinha.pt

### HTTP Request

`GET http://89.155.173.227:5000/api/url/<record_id>`

### Query Parameters

Parameter | Default | Description
--------- | ------- | -----------
url_id | 43 | record url that will be retrieved

<aside class="success">
Try out — <a href='http://89.155.173.227:5000/api/url?url_id=43'>http://89.155.173.227:5000/api/url</a>
</aside>

## Record From ID

> This method returns a JSON structured like this one:

```json
[
  {  
     "objecto_id":4,
     "dt_registo":1462752000000,
     "dt_actualizacao":1471305600000,
     "identifier":"0001",
     "designacao":null,
     "title":"Coleta da neblina",
     "creator":"Br\u00edgida Baltar",
     "subject":"Fotografia",
     (...)
     "tecnica":"Fotografia anal\u00f3gica colorida",
     "ficheiro":null
  }
]
```

This endpoint retrieves a specific record from In patrimonium Premium.

### HTTP Request

`GET http://89.155.173.227:5000/api/record/<record_id>`

### Query Parameters

Parameter | Default | Description
--------- | ------- | -----------
record_id | 4 | record that will be retrieved

<aside class="success">
Try out — <a href='http://89.155.173.227:5000/api/record?id=4'>http://89.155.173.227:5000/api/record</a>
</aside>

## Top Records

> This method returns a JSON structured like this one:

```json
{"id":4} {"id":5} {"id":6} {"id":7} {"id":8} 
{"id":9} {"id":10} {"id":12} {"id":13} {"id":14} 
{"id":16} {"id":17} {"id":18} {"id":19} {"id":20} 
{"id":22} {"id":23} {"id":26} {"id":25} {"id":11}
```

This endpoint retrieves the top record from In patrimonium Premium 

### HTTP Request

`GET http://89.155.173.227:5000/api/records/<total_records>`

### Query Parameters

Parameter | Default | Description
--------- | ------- | -----------
total_records | 100 | number of records to retrieve

<aside class="success">
Try out — <a href='http://89.155.173.227:5000/api/records?total_records=100'>http://89.155.173.227:5000/api/records</a>
</aside>

## Records by date

> This method returns a JSON structured like this one:

```json
{  
   "objecto_id":4,
   "dt_registo":1462752000000,
   "dt_actualizacao":1471305600000,
   "identifier":"0001",
   "designacao":null,
   "title":"Coleta da neblina",
   (...)
   "tecnica":"Fotografia anal\u00f3gica colorida",
   "ficheiro":null
}
{  
   "objecto_id":7,
   "dt_registo":1462752000000,
   "dt_actualizacao":1471305600000,
   "identifier":"0004",
   "designacao":null,
   "title":"Viva Maria",
   (...)
   "tecnica":null,
   "ficheiro":null
}
```

This endpoint retrieves the top record from In patrimonium Premium between dates

### HTTP Request

`GET http://89.155.173.227:5000/api/records_by_date/<total_records>`

### Query Parameters

Parameter | Default | Description
--------- | ------- | -----------
initial_date  | 07/04/1976 | initial date
final_date    | 07/04/2020 | end date

<aside class="success">
Try out — <a href='http://89.155.173.227:5000/api/records_by_date?initial_date=07/04/1976&final_date=07/04/2020'>http://89.155.173.227:5000/api/records_by_date</a>
</aside>

## Delete records

This endpoint deletes records between dates

### HTTP Request

`GET http://89.155.173.227:5000/api/delete_records/<total_records>`

### Query Parameters

Parameter | Default | Description
--------- | ------- | -----------
initial_date  | 07/04/1976 | initial date
final_date    | 07/04/1980 | end date

<aside class="success">
Try out — <a href='http://89.155.173.227:5000/api/delete_records?initial_date=07/04/1976&final_date=07/04/1980'>http://89.155.173.227:5000/api/delete_records</a>
</aside>

## Edited records

> This method returns a JSON structured like this one:

```json
{"objecto_id":4} {"objecto_id":5} {"objecto_id":6}
{"objecto_id":7} {"objecto_id":8} {"objecto_id":9}
{"objecto_id":10} {"objecto_id":12} {"objecto_id":13}
{"objecto_id":14} {"objecto_id":16} {"objecto_id":17}
{"objecto_id":18} {"objecto_id":19} {"objecto_id":20}
{"objecto_id":22} {"objecto_id":23} {"objecto_id":26}
{"objecto_id":25} {"objecto_id":11}
```

This endpoint retrives the edited records between dates

### HTTP Request

`GET http://89.155.173.227:5000/api/edited_records/<total_records>`

### Query Parameters

Parameter | Default | Description
--------- | ------- | -----------
initial_date  | 07/04/1976 | initial date
final_date    | 07/04/2060 | end date

<aside class="success">
Try out — <a href='http://89.155.173.227:5000/api/edited_records?initial_date=07/04/1976&final_date=07/04/2060'>http://89.155.173.227:5000/api/edited_records</a>
</aside>

## Cronology

> This method returns a JSON structured like this one:

```json
[
{  
   "objecto_id":4,
   "dt_registo":1462752000000,
   "dt_actualizacao":1471305600000,
   "identifier":"0001",
   (...)
   "cronologia":"1996",
   (...)
}{  
   "objecto_id":5,
   "dt_registo":1462752000000,
   "dt_actualizacao":1463097600000,
   "identifier":"0002",
   (...)
   "cronologia":"1996",
   (...)
}{  
   "objecto_id":6,
   "dt_registo":1462752000000,
   "dt_actualizacao":1463097600000,
   "identifier":"0003",
   (...)
   "cronologia":"1996",
   (...)
}
]
```

This endpoint retrieves the records from a 'cronology'

### HTTP Request

`GET http://89.155.173.227:5000/api/cronology/<cron>`

### Query Parameters

Parameter | Default | Description
--------- | ------- | -----------
cron | 1996 | cronology

<aside class="success">
Try out — <a href='http://89.155.173.227:5000/api/cronology?cron=1996'>http://89.155.173.227:5000/api/cronology</a>
</aside>

<!--

## Get a Specific Kitten

```ruby
require 'kittn'

api = Kittn::APIClient.authorize!('meowmeowmeow')
api.kittens.get(2)
```

```python
import kittn

api = kittn.authorize('meowmeowmeow')
api.kittens.get(2)
```

```shell
curl "http://example.com/api/kittens/2"
  -H "Authorization: meowmeowmeow"
```

```javascript
const kittn = require('kittn');

let api = kittn.authorize('meowmeowmeow');
let max = api.kittens.get(2);
```

> The above command returns JSON structured like this one:

```json
{
  "id": 2,
  "name": "Max",
  "breed": "unknown",
  "fluffiness": 5,
  "cuteness": 10
}
```

This endpoint retrieves a specific kitten.

<aside class="warning">Inside HTML code blocks like this one, you can't use Markdown, so use <code>&lt;code&gt;</code> blocks to denote code.</aside>

### HTTP Request

`GET http://example.com/kittens/<ID>`

### URL Parameters

Parameter | Description
--------- | -----------
ID | The ID of the kitten to retrieve

## Delete a Specific Kitten

```ruby
require 'kittn'

api = Kittn::APIClient.authorize!('meowmeowmeow')
api.kittens.delete(2)
```

```python
import kittn

api = kittn.authorize('meowmeowmeow')
api.kittens.delete(2)
```

```shell
curl "http://example.com/api/kittens/2"
  -X DELETE
  -H "Authorization: meowmeowmeow"
```

```javascript
const kittn = require('kittn');

let api = kittn.authorize('meowmeowmeow');
let max = api.kittens.delete(2);
```

> The above command returns JSON structured like this one:

```json
{
  "id": 2,
  "deleted" : ":("
}
```

This endpoint deletes a specific kitten.

### HTTP Request

`DELETE http://example.com/kittens/<ID>`

### URL Parameters

Parameter | Description
--------- | -----------
ID | The ID of the kitten to delete

-->
