!/bin/bash

topic=("topic1" "topic2" "topic1")
message=("apple" "banana" "orange")

for i in ${!topic[@]}; do
    echo "Starting publisher with topic: ${topic[$i]} and message: ${message[$i]}"
    go run cmd/publisher/example.go --topic=${topic[$i]} --message=${message[$i]} --transport=TCP &
done

wait

echo "All publishers have finished."
