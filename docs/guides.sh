echo "Usage: sh render.sh [publish]"
GUIDES=$HOME/docs/neo4j-guides
# git clone http://github.com/jexp/neo4j-guides $GUIDES

function render {
  $GUIDES/run.sh guides.adoc guides/index.html +1 "$@"
  $GUIDES/run.sh loadjdbc.adoc guides/loadjdbc.html +1 "$@"
  $GUIDES/run.sh loadjson.adoc guides/loadjson.html +1 "$@"
  $GUIDES/run.sh datetime.adoc guides/datetime.html +1 "$@"
  $GUIDES/run.sh loadxml.adoc guides/loadxml.html +1 "$@"
  $GUIDES/run.sh periodic.adoc guides/periodic.html +1 "$@"
}

mkdir guides
# -a env-training is a flag to enable full content, if you comment it out, the guides are rendered minimally e.g. for a presentation
if [ "$1" == "publish" ]; then
  URL=guides.neo4j.com/apoc
  render http://$URL
  s3cmd put --recursive -P guides/*.html img s3://${URL}/
  s3cmd put -P guides/index.html s3://${URL}

  echo "Publication Done"
else
  URL=localhost:8001/guides
# copy the csv files to $NEO4J_HOME/import
  render http://$URL -a csv-url=file:///
  echo "Starting Websever at $URL Ctrl-c to stop"
  python $GUIDES/http-server.py
  # python -m SimpleHTTPServer
fi
