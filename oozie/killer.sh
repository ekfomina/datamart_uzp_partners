# меняем статус загрузки на ERROR
current_date=$(date +'%Y-%m-%d %H:%M:%S')
curl -d  '{"loading_id": '$LOADING_ID', "effective_from": "'"$current_date"'", "status": "'"ERROR"'", "log": "'"exit code not 0"'"}' -H "Content-Type: application/json" -X PUT $CTL'/v1/api/loading/status'

# меняем состояние загрузки на ABORTED
curl -X DELETE $CTL'/v1/api/loading/'$LOADING_ID