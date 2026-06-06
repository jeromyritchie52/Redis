package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Database struct {
	mu      sync.RWMutex
	dict    map[string]string
	expires map[string]int64 // Unix timestamp in milliseconds
}

func (db *Database) expireIfNeeded(key string, now int64) bool {
	db.mu.RLock()
	expireTime, hasExpire := db.expires[key]
	db.mu.RUnlock()

	if !hasExpire {
		return false
	}

	if now > expireTime {
		db.mu.Lock()
		expireTime, hasExpire = db.expires[key]
		if hasExpire && now > expireTime {
			delete(db.dict, key)
			delete(db.expires, key)
			db.mu.Unlock()
			return true
		}
		db.mu.Unlock()
	}
	return false
}

type Server struct {
	dbs                     []*Database
	activeExpireEffort       int
	maxMemory                int64 // in bytes
	lastActiveExpireDbIndex  int
	mu                       sync.Mutex
}

func NewServer() *Server {
	dbs := make([]*Database, 16)
	for i := 0; i < 16; i++ {
		dbs[i] = &Database{
			dict:    make(map[string]string),
			expires: make(map[string]int64),
		}
	}
	return &Server{
		dbs:                dbs,
		activeExpireEffort: 4, // default effort (1-10)
		maxMemory:          512 * 1024 * 1024, // default 512MB
	}
}
unc (s *Server) Get(dbIdx int, key string) (string, bool) {
	db := s.dbs[dbIdx]
	now := time.Now().UnixNano() / int64(time.Millisecond)
	if db.expireIfNeeded(key, now) {
		return "", false
	}
	db.mu.RLock()
	val, found := db.dict[key]
	db.mu.RUnlock()
	return val, found
}

func (s *Server) Set(dbIdx int, key string, value string, ttl time.Duration, hasTTL bool) {
	db := s.dbs[dbIdx]
	db.mu.Lock()
	db.dict[key] = value
	if hasTTL {
		expireTime := time.Now().Add(ttl).UnixNano() / int64(time.Millisecond)
		db.expires[key] = expireTime
	} else {
		delete(db.expires, key)
	}
	db.mu.Unlock()
}

func (s *Server) Expire(dbIdx int, key string, ttl time.Duration) bool {
	db := s.dbs[dbIdx]
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, found := db.dict[key]; !found {
		return false
	}
	expireTime := time.Now().Add(ttl).UnixNano() / int64(time.Millisecond)
	db.expires[key] = expireTime
	return true
}

func (s *Server) Del(dbIdx int, key string) bool {
	db := s.dbs[dbIdx]
	db.mu.Lock()
	defer db.mu.Unlock()
	_, found := db.dict[key]
	if found {
		delete(db.dict, key)
		delete(db.expires, key)
	}
	return found
}

func (s *Server) FlushDB(dbIdx int) {
	db := s.dbs[dbIdx]
	db.mu.Lock()
	db.dict = make(map[string]string)
	db.expires = make(map[string]int64)
	db.mu.Unlock()
}

func (s *Server) FlushAll() {
	for i := 0; i < len(s.dbs); i++ {
		s.FlushDB(i)
	}
}

func (s *Server) SetActiveExpireEffort(effort int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeExpireEffort = effort
}

func (s *Server) GetActiveExpireEffort() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeExpireEffort
}

func (s *Server) SetMaxMemory(mem int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxMemory = mem
}

func (s *Server) GetMaxMemory() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxMemory
}

func (s *Server) activeExpireCycle() {
	s.mu.Lock()
	effort := s.activeExpireEffort
	maxMem := s.maxMemory
	dbIdx := s.lastActiveExpireDbIndex
	s.mu.Unlock()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memUsed := int64(m.Alloc)
	if maxMem > 0 && memUsed > maxMem {
		effort = 10
	}

	timeLimit := time.Duration(effort) * 250 * time.Microsecond
	start := time.Now()

	numDbs := len(s.dbs)
	dbsProcessed := 0

	for dbsProcessed < numDbs {
		db := s.dbs[dbIdx]

		for {
			if time.Since(start) >= timeLimit {
				s.mu.Lock()
				s.lastActiveExpireDbIndex = dbIdx
				s.mu.Unlock()
				return
			}

			sampleSize := 5 * effort
			if sampleSize < 10 {
				sampleSize = 10
			}

			expiredCount := 0
			sampledCount := 0

			db.mu.Lock()
			now := time.Now().UnixNano() / int64(time.Millisecond)
			keysToDelete := make([]string, 0, sampleSize)

			for key, expireTime := range db.expires {
				sampledCount++
				if now > expireTime {
					keysToDelete = append(keysToDelete, key)
					expiredCount++
				}
				if sampledCount >= sampleSize {
					break
				}
			}

			for _, key := range keysToDelete {
				delete(db.dict, key)
				delete(db.expires, key)
			}
			db.mu.Unlock()

			if sampledCount == 0 {
				break
			}

			expiredPercent := float64(expiredCount) / float64(sampledCount)
			if expiredPercent < 0.10 {
				break
			}
		}

		dbIdx = (dbIdx + 1) % numDbs
		dbsProcessed++
	}

	s.mu.Lock()
	s.lastActiveExpireDbIndex = dbIdx
	s.mu.Unlock()
}

func (s *Server) StartCron() {
	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		for range ticker.C {
			s.activeExpireCycle()
		}
	}()
}

type RESPReader struct {
	rd *bufio.Reader
}

func (r *RESPReader) ReadValue() (interface{}, error) {
	line, err := r.rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 {
		return nil, fmt.Errorf("invalid line")
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	if len(line) == 0 {
		return nil, fmt.Errorf("empty line")
	}

	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return errors.New(line[1:]), nil
	case ':':
		return strconv.ParseInt(line[1:], 10, 64)
	case '$':
		length, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if length == -1 {
			return nil, nil
		}
		buf := make([]byte, length+2)
		_, err = io.ReadFull(r.rd, buf)
		if err != nil {
			return nil, err
		}
		return string(buf[:length]), nil
	case '*':
		count, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if count == -1 {
			return nil, nil
		}
		arr := make([]interface{}, count)
		for i := 0; i < count; i++ {
			val, err := r.ReadValue()
			if err != nil {
				return nil, err
			}
			arr[i] = val
		}
		return arr, nil
	default:
		parts := strings.Fields(line)
		arr := make([]interface{}, len(parts))
		for i, part := range parts {
			arr[i] = part
		}
		return arr, nil
	}
}

func handleConnection(conn net.Conn, server *Server) {
	defer conn.Close()
	reader := &RESPReader{rd: bufio.NewReader(conn)}
	writer := bufio.NewWriter(conn)
	dbIndex := 0

	for {
		val, err := reader.ReadValue()
		if err != nil {
			return
		}
		arr, ok := val.([]interface{})
		if !ok || len(arr) == 0 {
			writer.WriteString("-ERR invalid command format\r\n")
			writer.Flush()
			continue
		}

		args := make([]string, len(arr))
		for i, v := range arr {
			s, ok := v.(string)
			if !ok {
				args[i] = ""
			} else {
				args[i] = s
			}
		}

		cmd := strings.ToUpper(args[0])
		switch cmd {
		case "PING":
			if len(args) == 1 {
				writer.WriteString("+PONG\r\n")
			} else if len(args) == 2 {
				writer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1]))
			} else {
				writer.WriteString("-ERR wrong number of arguments for 'ping' command\r\n")
			}
		case "SET":
			if len(args) < 3 {
				writer.WriteString("-ERR wrong number of arguments for 'set' command\r\n")
			} else {
				key := args[1]
				value := args[2]
				var ttl time.Duration
				hasTTL := false
				for i := 3; i < len(args); i++ {
					arg := strings.ToUpper(args[i])
					if arg == "EX" && i+1 < len(args) {
						sec, err := strconv.Atoi(args[i+1])
						if err == nil {
							ttl = time.Duration(sec) * time.Second
							hasTTL = true
						}
						i++
					} else if arg == "PX" && i+1 < len(args) {
						ms, err := strconv.Atoi(args[i+1])
						if err == nil {
							ttl = time.Duration(ms) * time.Millisecond
							hasTTL = true
						}
						i++
					}
				}
				server.Set(dbIndex, key, value, ttl, hasTTL)
				writer.WriteString("+OK\r\n")
			}
		case "GET":
			if len(args) != 2 {
				writer.WriteString("-ERR wrong number of arguments for 'get' command\r\n")
			} else {
				val, found := server.Get(dbIndex, args[1])
				if !found {
					writer.WriteString("$-1\r\n")
				} else {
					writer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
				}
			}
		case "EXPIRE":
			if len(args) != 3 {
				writer.WriteString("-ERR wrong number of arguments for 'expire' command\r\n")
			} else {
				sec, err := strconv.Atoi(args[2])
				if err != nil {
					writer.WriteString("-ERR value is not an integer or out of range\r\n")
				} else {
					ok := server.Expire(dbIndex, args[1], time.Duration(sec)*time.Second)
					if ok {
						writer.WriteString(":1\r\n")
					} else {
						writer.WriteString(":0\r\n")
					}
				}
			}
		case "DEL":
			if len(args) < 2 {
				writer.WriteString("-ERR wrong number of arguments for 'del' command\r\n")
			} else {
				count := 0
				for i := 1; i < len(args); i++ {
					if server.Del(dbIndex, args[i]) {
						count++
					}
				}
				writer.WriteString(fmt.Sprintf(":%d\r\n", count))
			}
		case "SELECT":
			if len(args) != 2 {
				writer.WriteString("-ERR wrong number of arguments for 'select' command\r\n")
			} else {
				idx, err := strconv.Atoi(args[1])
				if err != nil || idx < 0 || idx >= len(server.dbs) {
					writer.WriteString("-ERR invalid DB index\r\n")
				} else {
					dbIndex = idx
					writer.WriteString("+OK\r\n")
				}
			}
		case "FLUSHDB":
			server.FlushDB(dbIndex)
			writer.WriteString("+OK\r\n")
		case "FLUSHALL":
			server.FlushAll()
			writer.WriteString("+OK\r\n")
		case "CONFIG":
			if len(args) >= 4 && strings.ToUpper(args[1]) == "SET" {
				param := strings.ToLower(args[2])
				val := args[3]
				if param == "active-expire-effort" {
					effort, err := strconv.Atoi(val)
					if err == nil && effort >= 1 && effort <= 10 {
						server.SetActiveExpireEffort(effort)
						writer.WriteString("+OK\r\n")
					} else {
						writer.WriteString("-ERR invalid active-expire-effort value\r\n")
					}
				} else if param == "maxmemory" {
					mem, err := strconv.ParseInt(val, 10, 64)
					if err == nil {
						server.SetMaxMemory(mem)
						writer.WriteString("+OK\r\n")
					} else {
						writer.WriteString("-ERR invalid maxmemory value\r\n")
					}
				} else {
					writer.WriteString("-ERR unsupported config parameter\r\n")
				}
			} else if len(args) >= 3 && strings.ToUpper(args[1]) == "GET" {
				param := strings.ToLower(args[2])
				if param == "active-expire-effort" {
					effort := server.GetActiveExpireEffort()
					writer.WriteString(fmt.Sprintf("*2\r\n$20\r\nactive-expire-effort\r\n$%d\r\n%d\r\n", len(strconv.Itoa(effort)), effort))
				} else if param == "maxmemory" {
					mem := server.GetMaxMemory()
					writer.WriteString(fmt.Sprintf("*2\r\n$9\r\nmaxmemory\r\n$%d\r\n%d\r\n", len(strconv.FormatInt(mem, 10)), mem))
				} else {
					writer.WriteString("*0\r\n")
				}
			} else {
				writer.WriteString("-ERR unknown CONFIG subcommand\r\n")
			}
		default:
			writer.WriteString(fmt.Sprintf("-ERR unknown command '%s'\r\n", cmd))
		}
		writer.Flush()
	}
}

func main() {
	server := NewServer()
	server.StartCron()

	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Printf("Failed to bind to port 6379: %v\n", err)
		return
	}
	defer listener.Close()

	fmt.Println("Redis server listening on :6379")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}
		go handleConnection(conn, server)
	}
}