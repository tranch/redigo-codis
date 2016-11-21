package codis

import (
    "time"
    "testing"

    "github.com/garyburd/redigo/redis"
)

func TestGet(t *testing.T) {
    var (
        r redis.Conn
        p *Pool = &Pool{
            ZkServers: []string{"127.0.0.1:2181"},
            ZkTimeout: time.Second * 10,
            ZkDir: "/codis/proxy",
            Dial: func(network, address string) (redis.Conn, error) {
                conn, err := redis.Dial(network, address)
                if err == nil {
                    conn.Send("AUTH", "PASSWORD")
                }
                return conn, err
            },
        }

        test_key, test_value string = "key", "value"
    )

    r = p.Get()
    r.Do("SET", test_key, test_value)

    r = p.Get()
    result_value, _ := redis.String(r.Do("GET", test_key))

    if result_value != test_value {
        t.Failed()
    }
}
