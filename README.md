\# traccar-kafka-lab



Local GPS telemetry pipeline connecting \*\*Traccar\*\* -> \*\*Apache Kafka (KRaft via Podman)\*\* -> \*\*Python Consumers \& SQLite\*\*.



\## Prerequisites

1\. \*\*Python 3.9+\*\* (`pip install -r requirements.txt`)

2\. \*\*Podman\*\* (with WSL2 on Windows)

3\. \*\*Traccar Server\*\* installed locally



\## One-Time Traccar Setup

Add the following properties inside `<properties>` in your Traccar config (`C:\\Program Files\\Traccar\\conf\\traccar.xml` on Windows or `/opt/traccar/conf/traccar.xml` on Linux/macOS), then restart Traccar:



```xml

<entry key='forward.enable'>true</entry>

<entry key='forward.type'>kafka</entry>

<entry key='forward.url'>127.0.0.1:9092</entry>

<entry key='forward.topic'>traccar-positions</entry>

```



\## Quick Start



1\. \*\*Start Podman, Kafka, topics, and Traccar\*\* (use an Administrator terminal on Windows):

&#x20;  ```bash

&#x20;  python start.py

&#x20;  ```

2\. \*\*Start one or more consumer workers\*\* (up to 3 in parallel for 3 partitions):

&#x20;  ```bash

&#x20;  python consumer.py

&#x20;  ```

3\. \*\*Monitor overspeed alerts\*\* (in a separate terminal):

&#x20;  ```bash

&#x20;  python alert\_monitor.py

&#x20;  ```

4\. \*\*Simulate GPS traffic to Traccar\*\* (optionally pass a `.gpx` or `.geojson` route):

&#x20;  ```bash

&#x20;  python produce\_mock\_traccar\_events.py --route your\_track.gpx

&#x20;  ```

5\. \*\*Shut down all services and reclaim Podman VM memory:\*\*

&#x20;  ```bash

&#x20;  python stop.py

&#x20;  ```

