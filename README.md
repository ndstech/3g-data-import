### 3G Data Import


#### How to install
```
$ go get github.com/yogawa/3g-data-import
```


#### How to use
```
3g-data-import --connection "host=192.168.2.5 user=demo password=demo sslmode=disable" --db-name db_demo --table lte_hourly --file 20180801.csv --workers 4
```
