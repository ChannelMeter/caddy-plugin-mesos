package mesosproxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mesos/mesos-go/mesosproto"
	"github.com/mholt/caddy/config/setup"
	"github.com/mholt/caddy/middleware"
	"github.com/mholt/caddy/middleware/proxy"
	"github.com/samuel/go-zookeeper/zk"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var errInvalidPort = errors.New("Invalid port specified.")

type mesosUpstream struct {
	from        string
	mesosMaster string
	framework   string
	taskName    string
	hosts       *atomic.Value
	Policy      proxy.Policy

	FailTimeout time.Duration
	MaxFails    int32
	HealthCheck struct {
		Path     string
		Interval time.Duration
	}

	SyncInterval  time.Duration
	flushInterval time.Duration
	lastSync      time.Time
	syncing       int32
	syncWg        sync.WaitGroup

	Scheme string
	Port   int

	proxyHeaders http.Header
}

type mesosState struct {
	GitSha     string `json:"git_sha"`
	GitTag     string `json:"git_tag"`
	Leader     string `json:"leader"`
	Frameworks []struct {
		Id     string `json:"id"`
		Name   string `json:"name"`
		Active bool   `json:"active"`
		Tasks  []struct {
			Name      string `json:"name"`
			State     string `json:"state"`
			SlaveId   string `json:"slave_id"`
			Resources struct {
				Cpus  float64 `json:"cpus"`
				Disk  float64 `json:"disk"`
				Mem   float64 `json:"mem"`
				Ports string  `json:"ports"`
			} `json:"resources"`
		} `json:"tasks"`
	} `json:"frameworks"`
	Slaves []struct {
		Hostname string `json:"hostname"`
		Id       string `json:"id"`
	} `json:"slaves"`
}

// New creates a new instance of proxy middleware.
func Proxy(c *setup.Controller) (middleware.Middleware, error) {
	if upstreams, err := newMesosUpstreams(c); err == nil {
		return func(next middleware.Handler) middleware.Handler {
			return proxy.Proxy{Next: next, Upstreams: upstreams}
		}, nil
	} else {
		return nil, err
	}
}

func newMesosUpstreams(c *setup.Controller) ([]proxy.Upstream, error) {
	var upstreams []proxy.Upstream

	for c.Next() {
		upstream := &mesosUpstream{
			from:        "",
			hosts:       new(atomic.Value),
			Policy:      &proxy.Random{},
			FailTimeout: 10 * time.Second,
			MaxFails:    1,

			SyncInterval: 120 * time.Second,
			Scheme:       "http",
		}
		upstream.hosts.Store(proxy.HostPool([]*proxy.UpstreamHost{}))
		var proxyHeaders http.Header
		var port string
		if !c.Args(&upstream.from, &upstream.mesosMaster, &upstream.framework, &upstream.taskName, &port) {
			return upstreams, c.ArgErr()
		}
		if p, err := strconv.Atoi(port); err == nil {
			if p == 0 {
				return upstreams, errInvalidPort
			} else {
				upstream.Port = p
			}
		} else {
			return upstreams, err
		}

		for c.NextBlock() {
			switch c.Val() {
			case "policy":
				if !c.NextArg() {
					return upstreams, c.ArgErr()
				}
				switch c.Val() {
				case "random":
					upstream.Policy = &proxy.Random{}
				case "round_robin":
					upstream.Policy = &proxy.RoundRobin{}
				case "least_conn":
					upstream.Policy = &proxy.LeastConn{}
				default:
					return upstreams, c.ArgErr()
				}
			case "fail_timeout":
				if !c.NextArg() {
					return upstreams, c.ArgErr()
				}
				if dur, err := time.ParseDuration(c.Val()); err == nil {
					upstream.FailTimeout = dur
				} else {
					return upstreams, err
				}
			case "max_fails":
				if !c.NextArg() {
					return upstreams, c.ArgErr()
				}
				if n, err := strconv.Atoi(c.Val()); err == nil {
					upstream.MaxFails = int32(n)
				} else {
					return upstreams, err
				}
			case "health_check":
				if !c.NextArg() {
					return upstreams, c.ArgErr()
				}
				upstream.HealthCheck.Path = c.Val()
				upstream.HealthCheck.Interval = 30 * time.Second
				if c.NextArg() {
					if dur, err := time.ParseDuration(c.Val()); err == nil {
						upstream.HealthCheck.Interval = dur
					} else {
						return upstreams, err
					}
				}
			case "proxy_header":
				var header, value string
				if !c.Args(&header, &value) {
					return upstreams, c.ArgErr()
				}
				if proxyHeaders == nil {
					proxyHeaders = make(map[string][]string)
				}
				proxyHeaders.Add(header, value)
			case "sync_interval":
				if !c.NextArg() {
					return upstreams, c.ArgErr()
				}
				if dur, err := time.ParseDuration(c.Val()); err == nil {
					upstream.SyncInterval = dur
				} else {
					return upstreams, err
				}
			case "scheme":
				if !c.NextArg() {
					return upstreams, c.ArgErr()
				}
				upstream.Scheme = c.Val()
			case "flush_interval":
				if !c.NextArg() {
					return upstreams, c.ArgErr()
				}
				if dur, err := time.ParseDuration(c.Val()); err == nil {
					upstream.flushInterval = dur
				} else {
					return upstreams, err
				}
			}
		}
		upstream.proxyHeaders = proxyHeaders

		go upstream.syncWorker(nil)
		if upstream.HealthCheck.Path != "" {
			go upstream.healthCheckWorker(nil)
		}

		upstreams = append(upstreams, upstream)
	}
	return upstreams, nil
}

func (u *mesosUpstream) Hosts() proxy.HostPool {
	return u.hosts.Load().(proxy.HostPool)
}

func (u *mesosUpstream) healthCheck(hosts proxy.HostPool) {
	for _, host := range hosts {
		hostUrl := host.Name + u.HealthCheck.Path
		if r, err := http.Get(hostUrl); err == nil {
			io.Copy(ioutil.Discard, r.Body)
			r.Body.Close()
			host.Unhealthy = r.StatusCode < 200 || r.StatusCode >= 400
		} else {
			host.Unhealthy = true
		}
	}
}

func (u *mesosUpstream) healthCheckWorker(stop chan struct{}) {
	ticker := time.NewTicker(u.HealthCheck.Interval)
	u.healthCheck(u.Hosts())
	for {
		select {
		case <-ticker.C:
			u.healthCheck(u.Hosts())
		case <-stop:
			// TODO: the library should provide a stop channel and global
			// waitgroup to allow goroutines started by plugins a chance
			// to clean themselves up.
		}
	}
}

func (u *mesosUpstream) syncWorker(stop chan struct{}) {
	ticker := time.NewTicker(u.SyncInterval)
	u.sync()
	for {
		select {
		case <-ticker.C:
			u.sync()
		case <-stop:
			// TODO: the library should provide a stop channel and global
			// waitgroup to allow goroutines started by plugins a chance
			// to clean themselves up.
		}
	}
}

func (u *mesosUpstream) sync() {
	var syncing int32
	u.syncWg.Add(1)
	syncing = atomic.AddInt32(&u.syncing, 1)
	if syncing > 1 {
		atomic.AddInt32(&u.syncing, -1)
		u.syncWg.Done()
		u.syncWg.Wait()
		return
	}
	defer func() {
		u.syncWg.Done()
		atomic.AddInt32(&u.syncing, -1)
		u.lastSync = time.Now()
	}()
	var state mesosState
	var masterHosts []string

	if path, err := url.Parse(u.mesosMaster); err == nil {
		switch path.Scheme {
		case "zk":
			if path.Path == "" || path.Path == "/" {
				log.Printf("[ERROR] no path specified for mesos zk lookup \"%s\"", u.mesosMaster)
				return
			}
			zookeeperPath := path.Path
			if zookeeperPath[0] != '/' {
				zookeeperPath = "/" + zookeeperPath
			}
			if zoo, _, err := zk.Connect(strings.Split(path.Host, ","), 10*time.Second); err == nil {
				defer zoo.Close()
				if children, _, err := zoo.Children(zookeeperPath); err == nil {
					sort.Strings(children)
					for _, child := range children {
						if strings.HasPrefix(child, "info_") {
							if data, _, err := zoo.Get(zookeeperPath + "/" + child); err == nil {
								masterInfo := new(mesosproto.MasterInfo)
								if err := masterInfo.Unmarshal(data); err == nil {
									masterHosts = []string{fmt.Sprintf("%s:%d", masterInfo.GetHostname(), masterInfo.GetPort())}
									break
								} else {
									log.Printf("[ERROR] parsing mesos master from zookeeper. \"%s\"", err.Error())
									return
								}
							} else {
								log.Printf("[ERROR] getting mesos master from zookeeper. \"%s\"", err.Error())
								return
							}
						}
					}
				} else {
					log.Printf("[ERROR] getting mesos masters from zookeeper. \"%s\"", err.Error())
					return
				}
			}
		case "http", "https":
			masterHosts = strings.Split(path.Host, ",")
		default:
			log.Printf("[ERROR] unknown scheme in parsing mesos master url \"%s\"", u.mesosMaster)
			return
		}
	} else {
		masterHosts = strings.Split(u.mesosMaster, ",")
	}

	if len(masterHosts) == 0 {
		log.Printf("[ERROR] No reachable masters.")
		return
	}
	var masterErr error
	for _, host := range masterHosts {
		if resp, err := http.Get("http://" + host + "/state.json"); err == nil {
			defer resp.Body.Close()
			if err := json.NewDecoder(resp.Body).Decode(&state); err == nil {
				masterErr = nil
				break
			} else {
				masterErr = err
			}
		} else {
			masterErr = err
		}
	}
	if masterErr != nil {
		log.Printf("[ERROR] Failed to reach masters. \"%s\"", masterErr.Error())
		return
	}

	if len(state.Frameworks) == 0 {
		log.Println("[WARNING] No frameworks found running.")
		return
	}

	hosts := make(proxy.HostPool, 0, 4)
	for _, framework := range state.Frameworks {
		if framework.Name == u.framework {
			for _, task := range framework.Tasks {
				if task.Name == u.taskName && task.State == "TASK_RUNNING" {
					host := &proxy.UpstreamHost{
						Name:         task.SlaveId,
						Conns:        0,
						Fails:        0,
						FailTimeout:  u.FailTimeout,
						Unhealthy:    false,
						ExtraHeaders: u.proxyHeaders,
						CheckDown: func(upstream *mesosUpstream) proxy.UpstreamHostDownFunc {
							return func(uh *proxy.UpstreamHost) bool {
								if uh.Unhealthy {
									return true
								}
								if uh.Fails >= upstream.MaxFails &&
									upstream.MaxFails != 0 {
									return true
								}
								return false
							}
						}(u),
					}
					if u.Port > 0 {
						host.Name = host.Name + ":" + strconv.Itoa(u.Port)
					} else if u.Port < 0 {
						idx := (u.Port * -1) - 1
						if len(task.Resources.Ports) > 2 {
							portResource := task.Resources.Ports[1 : len(task.Resources.Ports)-1]
							ports := strings.Split(portResource, " ")
							if idx < len(ports) {
								selectedPort := ports[idx]
								if strings.Index(selectedPort, "-") != -1 {
									selectedPort = strings.Split(selectedPort, "-")[0]
									host.Name = host.Name + ":" + selectedPort
								}
							} else {
								continue
							}
						} else {
							continue
						}
					}
					hosts = append(hosts, host)
				}
			}
			break
		}
	}

	for _, host := range hosts {
		id, port := func() (string, string) {
			k := strings.Split(host.Name, ":")
			return k[0], k[1]
		}()
		for _, slave := range state.Slaves {
			if id == slave.Id {
				host.Name = u.Scheme + "://" + slave.Hostname + ":" + port
				break
			}
		}
	}
	oldPool := u.Hosts()
	if len(hosts) == 0 && len(oldPool) > 0 {
		return
	}
	isSame := len(oldPool) == len(hosts)
	for i, host := range hosts {
		found := false
		for _, oldHost := range oldPool {
			if oldHost.Name == host.Name {
				hosts[i] = oldHost
				found = true
				break
			}
		}
		if !found {
			isSame = false
		}
	}

	for _, host := range hosts {
		if host.ReverseProxy == nil {
			if baseUrl, err := url.Parse(host.Name); err == nil {
				host.ReverseProxy = proxy.NewSingleHostReverseProxy(baseUrl, "")
				host.ReverseProxy.FlushInterval = u.flushInterval
			} else {
				return
			}
		}
	}

	if !isSame {
		if u.HealthCheck.Path != "" {
			u.healthCheck(hosts)
		}
		u.hosts.Store(hosts)
	}
}

func (u *mesosUpstream) From() string {
	return u.from
}

func (u *mesosUpstream) Select() *proxy.UpstreamHost {
	for i := 0; i < 2; i++ {
		pool := u.Hosts()
		if len(pool) == 0 {
			u.sync()
			pool = u.Hosts()
		}
		if len(pool) == 1 {
			if pool[0].Down() {
				u.sync()
				continue
			}
			return pool[0]
		}
		allDown := true
		for _, host := range pool {
			if !host.Down() {
				allDown = false
				break
			}
		}
		if allDown {
			u.sync()
			continue
		}

		if u.Policy == nil {
			return (&proxy.Random{}).Select(pool)
		} else {
			return u.Policy.Select(pool)
		}
	}
	return nil
}
