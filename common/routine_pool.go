package common

import (
    log "github.com/golang/glog"
    "github.com/panjf2000/ants/v2"
    "time"
)

type PoolOption struct {
    ExpiryDuration time.Duration
}

type RoutinePool struct {
    pool *ants.Pool
}

func NewPool(size int32, option ...PoolOption) (*RoutinePool, error) {
    if size <= 0 {
        size = 1
    }
    if len(option) == 0 {
        originalPool, err := ants.NewPool(int(size), ants.WithNonblocking(true),
            ants.WithPanicHandler(func(panicInfo interface{}) {
                log.Error(panicInfo)
            }))
        return &RoutinePool{pool: originalPool}, err
    }
    firstOption := option[0]
    if firstOption.ExpiryDuration <= 0 {
        firstOption.ExpiryDuration = time.Second
    }
    originalPool, err := ants.NewPool(int(size), ants.WithNonblocking(true),
        ants.WithExpiryDuration(firstOption.ExpiryDuration),
        ants.WithPanicHandler(func(panicInfo interface{}) {
            log.Error(panicInfo)
        }))
    return &RoutinePool{pool: originalPool}, err
}

func NewEmptyPool() *RoutinePool {
    return &RoutinePool{pool: nil}
}

func executeTaskWithCaller(task func()) {
    defer func() {
        if p := recover(); p != nil {
            log.Errorf("Runtime error caught:%v", p)
        }
    }()
    // 调用者线程执行
    task()
}

func (routinePool *RoutinePool) Submit(task func()) {
    if routinePool.pool == nil {
        executeTaskWithCaller(task)
        return
    }
    err := routinePool.pool.Submit(task)
    if err != nil {
        log.Error(err.Error())
        executeTaskWithCaller(task)
    }
}

func (routinePool *RoutinePool) Close() {
    if routinePool.pool == nil {
        return
    }
    if !routinePool.pool.IsClosed() {
        routinePool.pool.Release()
    }
}
