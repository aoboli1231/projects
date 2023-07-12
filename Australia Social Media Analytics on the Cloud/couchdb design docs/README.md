# Contributor

COMP90024 Project 2 2023

- Aobo Li (1172339)
- Pavith Samarakoon (1297058)
- Zhihao Liang (1367102)
- Jiqiang Chen (1171420)
- Yijun Liu (1132416)

## How to Put Design Docs into CouchDB

To put design documents into CouchDB, follow the steps below:

```shell
curl -X PUT http://{username}:{password}@{localhost}/{database}/_design/{design_doc_name} -d @{filename}.json
```

## To get MapReduce results

```shell
curl -X GET 'http://{username}:{password}@{localhost}/{database}/_design/{design_doc_name}/_view/{view_name}?group=true'
```


Make sure to replace the placeholders with the appropriate values:

- `{username}`: Your CouchDB username
- `{password}`: Your CouchDB password
- `{localhost}`: The URL or IP address of your CouchDB server
- `{database}`: The name of the target database
- `{design_doc_name}`: The name you want to assign to the design document
- `{filename}`: The name of the JSON file containing the design document
- `{view_name}`: The name of the view file

This command will perform an HTTP `PUT` request to upload the design document to CouchDB.

For more information about the `PUT` request in CouchDB, you can visit the [CouchDB Documentation](https://docs.couchdb.org/en/stable/api/database/common.html#put--db-docid) page.

Feel free to modify and extend the source code as needed for your specific requirements.
