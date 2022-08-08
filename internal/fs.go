package httpfs

import (
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
	"github.com/winfsp/cgofuse/fuse"
)

type HttpFs struct {
	fuse.FileSystemBase
	client     http.Client
	baseurl    string
	block_size int64
	lru_cache  *lru.Cache
	logger     *log.Logger
}

type Profile struct {
	Base       string
	MountPoint string
	CacheDir   string
	LogDir     string
	BlockSize  int64
	CacheSize  int
}

func NewHttpFs(profile *Profile) *HttpFs {
	block_size := int64(math.Pow(2, 22))
	cache_size := 400
	if profile.BlockSize != 0 {
		block_size = profile.BlockSize
	}
	if profile.CacheSize > 0 {
		cache_size = profile.CacheSize
	}
	client := http.Client{
		Timeout: time.Duration(20) * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}, // don't follow redirects
	}
	cache, _ := lru.New(cache_size)
	logger := log.New()
	return &HttpFs{
		client:     client,
		baseurl:    strings.TrimRight(profile.Base, "/"),
		block_size: block_size,
		lru_cache:  cache,
		logger:     logger,
	}
}

func (self *HttpFs) Open(path string, flags int) (errc int, fh uint64) {
	return 0, 0
}

func (self *HttpFs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	if path == "/" {
		stat.Mode = fuse.S_IFDIR | 0755
		return 0
	}
	req, _ := http.NewRequest("HEAD", self.baseurl+path, nil)
	req.Header.Set("User-Agent", "HttpFS/0.0.1")
	resp, err := self.client.Do(req)
	if err != nil || resp.StatusCode == 404 {
		return -fuse.ENOENT
	}
	content_length, err := strconv.ParseInt(resp.Header.Get("content-length"), 10, 64)
	self.logger.Infof("Getattr [%d] %s <%d>", resp.StatusCode, path, content_length)
	_, err = resp.Location()
	if err != nil {
		stat.Mode = fuse.S_IFREG | 0644
		stat.Size = content_length
		return 0
	} else {
		stat.Mode = fuse.S_IFDIR | 0755
		return 0
	}
}

func (self *HttpFs) Read(path string, buff []byte, ofst int64, fh uint64) (n int) {
	endofst := ofst + int64(len(buff))
	channel := make(chan *Block)
	defer close(channel)
	block_cnt := int64(0)
	block_start_id := int64(ofst/self.block_size) + 1
	for i := block_start_id; ; i++ {
		_, end := self.getBlockRange(i)
		go self.getBlock(path, i, channel)
		if end >= endofst {
			block_cnt = i - block_start_id + 1
			break
		}
	}
	block_map := make(map[int64]int)
	current := ofst
	self.logger.Infof("Read %s <%d-%d>", path, ofst, endofst)
	for i := int64(0); i < block_cnt; i++ {
		blk := <-channel
		if blk.err != nil && blk.err != io.EOF {
			self.logger.Error(blk.err)
			continue
		}
		_, end := self.getBlockRange(blk.block_id)
		if int(current%self.block_size) <= len(blk.data) {
			block_map[blk.block_id] = copy(buff[current-ofst:], blk.data[current%self.block_size:])
		} else {
			block_map[blk.block_id] = 0
		}
		current = end
	}
	n = 0
	current = ofst
	for i := block_start_id; i <= block_start_id+block_cnt; i++ {
		if size, ok := block_map[i]; ok {
			n += size
		} else {
			break
		}
	}

	return
}

func (self *HttpFs) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) (errc int) {
	fill(".", nil, 0)
	fill("..", nil, 0)
	return 0
}
