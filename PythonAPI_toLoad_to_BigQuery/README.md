## Using Django Rest Framework API with YAML Configuration to load the json data into BIG Query


To test, 
1) first:  python manage.py runserver
2) second: execute below curl command
            curl -X POST "http://127.0.0.1:8000/api/load-json/" \
        -H "Content-Type: application/json" \
        -d '{
            "records": [
                {
                    "name": "John Doe",
                    "age": 30,
                    "email": "john.doe@example.com"
                },
                {
                    "name": "Jane Smith",
                    "age": 25,
                    "email": "jane.smith@example.com"
                }
            ]
            }'

