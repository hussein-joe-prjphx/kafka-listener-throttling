for i in {1..10} ; do
    curl -X POST http://localhost:8080/send/batch/"${i}"
done