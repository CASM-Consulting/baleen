# Baleen

See readme of upstream project [here ](https://github.com/dstl/baleen).

# Getting Started

Run `cd baleen; mvn package` to build an executable, then `java -jar target/baleen-2.2.0-SNAPSHOT.jar sussex/confs/runner-miro.yaml` to start the web server.

Once running, the server can be accessed at [http://localhost:6413](http://localhost:6413). Use HTTP POST requests to have Baleen annotate data for you:

```
wget http://0.0.0.0:3124/sussex/consume --post-data='data=[{"text":"hello from www.google.com in Germany","id":"4"},{"text":"hello from www.google.com in Germany and drink 2 pints of water every day","id":"3"}]' -qO-
```

Responses are in the form

```
[{"text":"hello from www.google.com in Germany","id":"1","locations":[],"urls":["www.google.com"],"quantities":[]},{"text":"hello from www.google.com in Germany and drink 2 pints of water every day","id":"2","locations":[],"urls":["www.google.com"],"quantities":["2 pints"]}]
```

Currently only quantities, URLs and locations are extracted and returned.



