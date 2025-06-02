## How to run


Download maelstrom from [jepsen-io/maelstrom](https://github.com/jepsen-io/maelstrom/releases) and unpack it.

From materialize root start up minio, compile persistcli and launch maelstrom. Note that this assumes you downloaded and unpacked maelstrom in ~/Downloads, if not, adjust the path.
```
# docker compose -f src/persist/s3_consensus_docker/docker-compose.yml up -d
# cargo build --bin persistcli && java -Djava.awt.headless=true -jar ~/Downloads/maelstrom/lib/maelstrom.jar test -w txn-list-append --bin ./target/debug/persistcli -- maelstrom --consensus-uri='s3://demo:demodemo123@demo/p1/?endpoint=http%3A%2F%2F127.0.0.1%3A9000&region=us-east-1' --blob-uri='mem://'
```

When you feel like the world is a better place because of your hard work, shut things down:
```
# docker compose -f src/persist/s3_consensus_docker/docker-compose.yml down
```
