package pool

import (
	"context"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

//NSQ Producer Pool
type NSQProducerPool struct {
	Mu          sync.Mutex
	IdleTimeout time.Duration
	conns       chan *nsqIdleConn
	factory     func() (*nsq.Producer, error)
	close       func(*nsq.Producer) error
}

type nsqIdleConn struct {
	conn *nsq.Producer
	t    time.Time
}

//Get get from pool
func (c *NSQProducerPool) Get() (*nsq.Producer, error) {
	c.Mu.Lock()
	conns := c.conns
	c.Mu.Unlock()

	if conns == nil {
		return nil, errClosed
	}
	for {
		select {
		case wrapConn := <-conns:
			if wrapConn == nil {
				return nil, errClosed
			}
			//判断是否超时，超时则丢弃
			if timeout := c.IdleTimeout; timeout > 0 {
				if wrapConn.t.Add(timeout).Before(time.Now()) {
					//丢弃并关闭该链接
					c.close(wrapConn.conn)
					continue
				}
			}
			return wrapConn.conn, nil
		default:
			conn, err := c.factory()
			if err != nil {
				return nil, err
			}

			return conn, nil
		}
	}
}

//Put put back to pool
func (c *NSQProducerPool) Put(conn *nsq.Producer) error {
	if conn == nil {
		return errRejected
	}

	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.conns == nil {
		return c.close(conn)
	}

	select {
	case c.conns <- &nsqIdleConn{conn: conn, t: time.Now()}:
		return nil
	default:
		//连接池已满，直接关闭该链接
		return c.close(conn)
	}
}

//Close close pool
func (c *NSQProducerPool) Close() {
	c.Mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	closeFun := c.close
	c.close = nil
	c.Mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for wrapConn := range conns {
		closeFun(wrapConn.conn)
	}
}

//IdleCount idle connection count
func (c *NSQProducerPool) IdleCount() int {
	c.Mu.Lock()
	conns := c.conns
	c.Mu.Unlock()
	return len(conns)
}

//init nsq producer pool
func NewNSQProducerPool(o *Options) (*NSQProducerPool, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}

	//init pool
	pool := &NSQProducerPool{
		conns: make(chan *nsqIdleConn, o.MaxCap),
		factory: func() (*nsq.Producer, error) {
			target := o.nextTarget()
			if target == "" {
				return nil, errTargets
			}

			_, cancel := context.WithTimeout(context.Background(), o.DialTimeout)
			defer cancel()
			config := nsq.NewConfig()

			return nsq.NewProducer(o.InitTargets[0], config)
		},
		close: func(v *nsq.Producer) error {
			v.Stop()
			return nil
		},
		IdleTimeout: o.IdleTimeout,
	}

	//danamic update targets
	o.update()

	//init make conns
	for i := 0; i < o.InitCap; i++ {
		conn, err := pool.factory()
		if err != nil {
			pool.Close()
			return nil, err
		}
		pool.conns <- &nsqIdleConn{conn: conn, t: time.Now()}
	}

	return pool, nil
}
