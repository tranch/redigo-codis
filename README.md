# Redigo Codis

Redigo Codis is a go client for codis based on Redigo.

## Features

- Use a round robin policy to balance load to multiple codis proxies.
- Detect proxy online and offline automatically.

## How to use

```go
import (
    "time"

    "github.com/tranch/redigo-codis"
)

p := &codis.Pool{
    ZkServers: "localhost:2181",
    ZkTimeout: time.Seconds * 60,
    ZkPath: "/codis/proxy",
    Dail: func(network, address string) (redis.Conn, error) {
        conn, err := redis.Dial(network, address)
        if err == nil {
            conn.Send("AUTH", "PASSWORD")
        }
        return conn, err
    }
}

r = p.Get()
r.Do("SET", "key", "value")
```
