package codis

import (
    "time"
    "sync"
    "errors"
    "encoding/json"

    "github.com/samuel/go-zookeeper/zk"
    "github.com/garyburd/redigo/redis"
    "github.com/op/go-logging"
)

type proxyInfo struct {
    Proto_type   string
    Proxy_addr   string
}

type Pool struct {
    nextIdx   int
    pools     []redis.Conn
    zk        zk.Conn
    ZkServers []string
    ZkTimeout time.Duration
    ZkDir     string
    Dial      func(network, address string) (redis.Conn, error)
}

var (
    mu = &sync.Mutex{}
    Log *logging.Logger = logging.MustGetLogger("Codis")
)

func (this *Pool) initFromZk() {
    this.initZk()
    this.pools = []redis.Conn{}
    children, _, err := this.zk.Children(this.ZkDir)

    if err != nil {
        Log.Fatal(err)
    }

    for _, child := range children {
        data, _, err := this.zk.Get(this.ZkDir + "/" + child)

        if err != nil {
            continue
        }

        var p proxyInfo

        json.Unmarshal(data, &p)
        conn, err := this.Dial(p.Proto_type, p.Proxy_addr)
        if err != nil {
            Log.Errorf("Create redis connection failed: %s", err.Error())
            continue
        }
        this.pools = append(this.pools, conn)
    }

    go this.watch(this.ZkDir)
}

func (this *Pool) watch(node string) {
    for {
        _, _, ch, err := this.zk.ChildrenW(node)
        if err != nil {
            Log.Error(err)
            return
        }
        evt := <-ch

        if evt.Type == zk.EventSession {
            if evt.State == zk.StateConnecting {
                continue
            }
            if evt.State == zk.StateExpired {
                this.zk.Close()
                Log.Info("Zookeeper session expired, reconnecting...")
                this.initZk()
            }
        }
        if evt.State == zk.StateConnected {
            switch evt.Type {
            case
                zk.EventNodeCreated,
                zk.EventNodeDeleted,
                zk.EventNodeChildrenChanged,
                zk.EventNodeDataChanged:
                this.initFromZk()
                return
            }
            continue
        }
    }
}

func (this *Pool) initZk() {
    zkConn, _, err := zk.Connect(this.ZkServers, this.ZkTimeout)
    if err != nil {
        Log.Fatalf("Failed to connect to zookeeper: %+v", err)
    }
    this.zk = *zkConn
}

func (this *Pool) Get() redis.Conn {
    mu.Lock()
    if len(this.pools) == 0 {
        this.initFromZk()
    }
    this.nextIdx += 1
    if this.nextIdx >= len(this.pools) {
        this.nextIdx = 0
    }
    if len(this.pools) == 0 {
        mu.Unlock()
        err := errors.New("Proxy list empty")
        Log.Error(err)
        return errorConnection{err: err}
    } else {
        c := this.pools[this.nextIdx]
        mu.Unlock()
        return c
    }
}

func (this *Pool) Close() {
    this.zk.Close()
    this.pools = []redis.Conn{}
}

type errorConnection struct{ err error }

func (ec errorConnection) Do(string, ...interface{}) (interface{}, error) { return nil, ec.err }
func (ec errorConnection) Send(string, ...interface{}) error              { return ec.err }
func (ec errorConnection) Err() error                                     { return ec.err }
func (ec errorConnection) Close() error                                   { return ec.err }
func (ec errorConnection) Flush() error                                   { return ec.err }
func (ec errorConnection) Receive() (interface{}, error)                  { return nil, ec.err }
