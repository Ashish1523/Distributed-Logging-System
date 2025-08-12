from elasticsearch import Elasticsearch

# Elasticsearch configuration
es = Elasticsearch(
    ['https://localhost:9200'],  # Elasticsearch server URL
    basic_auth=('elastic', 'LhWKa6e=giW*GeiCHDHt'),  # Replace with your username and password
    verify_certs=False  # Disable SSL verification for self-signed certificates
)

# Function to retrieve all logs
def retrieve_all_logs(index_name):
    try:
        # Fetch all documents from the specified index
        response = es.search(
            index=index_name,
            body={
                "query": {
                    "match_all": {}  # Retrieve all documents
                }
            },
            size=10000  # Adjust size for larger datasets (max 10,000 in one request)
        )
        
        # Check if there are hits and print them
        if 'hits' in response and 'hits' in response['hits']:
            print(f"Total Logs Retrieved: {response['hits']['total']['value']}")
            for hit in response['hits']['hits']:
                print(hit['_source'])  # Print the log content
        else:
            print("No logs found.")
    
    except Exception as e:
        print(f"Error retrieving logs: {e}")

# Retrieve logs from 'service-logs' index
retrieve_all_logs('service-logs')
