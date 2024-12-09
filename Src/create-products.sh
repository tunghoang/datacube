hourly_4km=`cat 4km_hourly`
hourly_10km=`cat 10km_hourly`
daily_10km=`cat 10km_daily`

function product_create {
	curl -X POST -H 'Content-Type: application/json' --data "{\"name\":\"$1\", \"resolution\":$2, \"frequency\":\"$3\"}" http://127.0.0.1:9000/products
}

for p in $hourly_4km; do
	product_create $p 4 hourly
done
for p in $hourly_10km; do
	product_create $p 10 hourly
done
for p in $daily_10km; do
	product_create $p 10 daily
done
