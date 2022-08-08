package httpfs

import (
	"fmt"
	"io"
	"net/http"
)

type Block struct {
	block_id int64
	data     []byte
	err      error
}

func (self *HttpFs) getRange(path string, start int64, end int64) ([]byte, error) {
	req, _ := http.NewRequest("GET", self.baseurl+path, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end-1))
	req.Header.Set("User-Agent", "HttpFS/0.0.1")
	resp, err := self.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (self *HttpFs) getBlock(path string, block_num int64, ch chan *Block) (data []byte, err error) {
	defer func() {
		ch <- &Block{
			block_id: block_num,
			data:     data,
			err:      err,
		}
	}()
	key := fmt.Sprintf("%s-%d-%d", path, self.block_size, block_num)

	if cached, ok := self.lru_cache.Get(key); ok {
		data, err = cached.([]byte), nil
		return
	}
	start, end := self.getBlockRange(block_num)
	data, err = self.getRange(path, start, end)
	if err == nil {
		self.lru_cache.Add(key, data)
	}
	return
}

func (self *HttpFs) getBlockRange(block_num int64) (start int64, end int64) {
	return (block_num - 1) * self.block_size, block_num * self.block_size
}
