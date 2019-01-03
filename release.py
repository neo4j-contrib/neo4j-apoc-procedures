import json
import os
import requests
import sys


def main(token, tag_name, file_name):
    headers = {"Authorization": "bearer {token}".format(token=token)}
    data = {'tag_name': tag_name}
    response = requests.post("https://api.github.com/repos/neo4j-contrib/neo4j-apoc-procedure/releases",
                             data=json.dumps(data), headers=headers)
    release_json = response.json()
    print(release_json)
    release_id = release_json["id"]

    with open(file_name, "rb") as file_name_handle:
        upload_url = "https://uploads.github.com/repos/neo4j-contrib/neo4j-apoc-procedures/releases/{release_id}/assets?name={file_name}".format(
            release_id=release_id, file_name=file_name.split("/")[-1]
        )
        print(upload_url)

        headers["Content-Type"] = "application/java-archive"

        response = requests.post(upload_url, headers=headers, data=file_name_handle.read())
        print(response.text)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python release.py [tag_name] [file_name]")
        sys.exit(1)

    token = os.getenv("GITHUB_TOKEN")
    tag_name = sys.argv[1]
    file_name = sys.argv[2]
    main(token, tag_name, file_name)
