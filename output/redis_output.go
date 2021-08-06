package output

import (
    "fmt"
    "github.com/go-redis/redis"
    "github.com/golang/glog"
    "github.com/kevinu2/gohangout/codec"
    "github.com/kevinu2/gohangout/topology"
)

func init() {
    Register("RedisPub", newRedisOutput)
}

type RedisOutput struct {
    config      map[interface{}]interface{}
    encoder     codec.Encoder
    client      *redis.Client
    addr        string
    passwd      string
    db          int
    channelName string
}

func newRedisOutput(config map[interface{}]interface{}) topology.Output {
    p := &RedisOutput{
        config: config,
    }

    if v, ok := config["codec"]; ok {
        p.encoder = codec.NewEncoder(v.(string))
    } else {
        p.encoder = codec.NewEncoder("json")
    }

    if v, ok := config["addr"]; ok {
        if p.addr, ok = v.(string); !ok {
            glog.Fatal("redis addr must be a string")
        }
    } else {
        glog.Fatal("addr must be set in redis output")
    }
    if v, ok := config["passwd"]; ok {
        if p.passwd, ok = v.(string); !ok {
            glog.Fatal("redis passwd must be a string")
        }
    } else {
        glog.Fatal("passwd must be set in redis output")
    }
    if v, ok := config["db"]; ok {
        if p.db, ok = v.(int); !ok {
            glog.Fatal("redis passwd must be a string")
        }
    } else {
        glog.Fatal("passwd must be set in redis output")
    }
    if v, ok := config["channel"]; ok {
        if p.channelName, ok = v.(string); !ok {
            glog.Fatal("redis channel must be a string")
        }
    } else {
        glog.Fatal("channel must be set in redis output")
    }
    //RedisDB.AddConfig("", "", "passwd", "host", port, "dbTime string", 0, 0, 0, 0)
    /*p.redisdb = redis.NewClient(&redis.Options{
          Addr: p.addr,Password: p.passwd, DB: p.db,
      })
      if p.redisdb == nil{
          glog.Fatal("output redis init err")
      }*/
    //p.cacher = RedisDB.GetDB()
    p.client = redis.NewClient(&redis.Options{
        Addr:     p.addr,
        Password: p.passwd,
        DB:       p.db,
        PoolSize: 10,
    })
    return p

}

func (p *RedisOutput) Emit(event map[string]interface{}) {
    buf, err := p.encoder.Encode(event)
    if err != nil {
        glog.Errorf("marshal %v error:%s", event, err)
    }
    //fmt.Println(string(buf))
    //p.cacher.Publish("", string(buf))
    //p.redisdb.Publish(p.channelName, string(buf))
    _, err = p.client.Publish(p.channelName, string(buf)).Result()
    if err != nil {
        fmt.Printf(err.Error())
        return
    }
}

func (p *RedisOutput) Shutdown() {
    /*if p.redisdb != nil{
    	p.redisdb.Close()
    }*/
    p.client.Close()
}
