#!/bin/sh

curl -X POST --data-urlencode "payload={\"channel\": \"#metabase_files\", \"username\": \"batch\", \"text\": \"MetaBoard report: $1\", \"icon_emoji\": \":ghost:\"}" https://hooks.slack.com/services/abc/xyz/1234