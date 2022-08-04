package pkg

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	nebula_go "github.com/vesoft-inc/nebula-go/v3"
)

type (
	Client struct {
		sessionPool *sessionPool
	}
	sessionPool struct {
		nebulaPool *nebula_go.ConnectionPool
		sessions   map[int]*nebula_go.Session
		lock       sync.Mutex
		opts       *Options
	}
)

var pool *sessionPool

const (
	defaultMaxConnSize = 200
	defaultMinConnSize = 10
	defaultTimeout     = 0
)

func NewClient() *Client {
	return &Client{}
}

func (c *Client) Query(stmt string) (*nebula_go.ResultSet, error) {
	if pool == nil {
		sp, err := initPool(Opts)
		if err != nil {
			return nil, err
		}
		pool = sp
	}
	var (
		sess *nebula_go.Session
		err  error
	)
	for i := 0; i < defaultMinConnSize+1; i++ {
		sess, err = pool.borrow()
		if err != nil {
			return nil, err
		}
		//ping
		if resp, err := sess.Execute("yield 1"); err != nil {
			sess = nil
		} else {
			if !resp.IsSucceed() {
				Logger.Warningf("cannot ping the session, err: %s", resp.GetErrorMsg())
				continue
			}
			break
		}
	}
	if sess == nil {
		return nil, fmt.Errorf("cannot ping the session")
	}
	defer pool.back(sess)

	return sess.Execute(stmt)

}

func initPool(opts *Options) (*sessionPool, error) {
	c := &nebula_go.PoolConfig{}
	c.MaxConnPoolSize = defaultMaxConnSize
	c.MinConnPoolSize = defaultMinConnSize
	c.TimeOut = defaultTimeout
	s, err := getHostAddress(opts.Address)
	if err != nil {
		return nil, err
	}

	nebulaPool, err := nebula_go.NewConnectionPool([]nebula_go.HostAddress{*s}, *c, &nebula_go.DefaultLogger{})
	if err != nil {
		return nil, err
	}
	sessions := make(map[int]*nebula_go.Session, 0)
	for i := 0; i < defaultMinConnSize; i++ {
		sess, err := nebulaPool.GetSession(opts.User, opts.Password)
		if err != nil {
			return nil, err
		}
		r, err := sess.Execute(fmt.Sprintf("USE %s", opts.Space))
		if err != nil {
			return nil, err
		}
		if !r.IsSucceed() {
			return nil, fmt.Errorf("cannot use the space: %s", opts.Space)
		}
		sessions[i] = sess
	}
	return &sessionPool{nebulaPool: nebulaPool, sessions: sessions, opts: opts}, nil
}

func getHostAddress(address string) (*nebula_go.HostAddress, error) {
	ss := strings.Split(address, ":")
	if len(ss) != 2 {
		return nil, fmt.Errorf("cannot parse the address string")
	}
	port, err := strconv.Atoi(ss[1])
	if err != nil {
		return nil, err
	}
	return &nebula_go.HostAddress{Host: ss[0], Port: port}, nil
}

func (sp *sessionPool) borrow() (*nebula_go.Session, error) {
	sp.lock.Lock()
	defer sp.lock.Unlock()
	var s nebula_go.Session

	if len(sp.sessions) > 0 {
		s = *sp.sessions[len(sp.sessions)-1]
		delete(sp.sessions, len(sp.sessions)-1)
	} else {
		// create a new session
		sess, err := sp.nebulaPool.GetSession(sp.opts.User, sp.opts.Password)
		if err != nil {
			return nil, err
		}
		r, err := sess.Execute(fmt.Sprintf("USE %s", sp.opts.Space))
		if err != nil {
			return nil, err
		}
		if !r.IsSucceed() {
			return nil, fmt.Errorf("cannot use the space: %s", sp.opts.Space)
		}
		s = *sess
	}
	return &s, nil
}

func (sp *sessionPool) back(sess *nebula_go.Session) {
	sp.lock.Lock()
	defer sp.lock.Unlock()
	n := len(sp.sessions)
	sp.sessions[n] = sess
}

func CloseSessions() {
	if pool == nil {
		return
	}
	pool.lock.Lock()
	defer pool.lock.Unlock()
	for _, sess := range pool.sessions {
		sess.Release()
	}
}
