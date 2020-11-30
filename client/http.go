package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync/atomic"
)

type httpClient struct {
	cli   *http.Client
	conf  Config
	urls  []string
	pos   int32
	total int32
}

func NewHttpClient(conf Config) Client {
	c := &httpClient{
		cli:  http.DefaultClient,
		conf: conf,
	}
	for _, endpoint := range conf.Endpoints {
		url := fmt.Sprint("http://", endpoint, conf.RequestPath, "?", conf.Query, "=%s")
		c.urls = append(c.urls, url)
	}
	c.total = int32(len(c.urls))
	return c
}

func (c *httpClient) GetId(key string) (int64, error) {
	url := fmt.Sprintf(c.geturl(), key)
	resp, err := c.cli.Get(url)
	if err != nil {
		return -1, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return -2, err
	}

	r := response{}
	err = json.Unmarshal(data, &r)
	if err != nil {
		return -3, err
	}
	if r.Id <= 0 || r.Msg != "" {
		return -4, fmt.Errorf("id:%d, err:%s", r.Id, r.Msg)
	}
	return r.Id, nil
}

type response struct {
	Id  int64  `json:"id"`
	Msg string `json:"msg"`
}

func (c *httpClient) geturl() string {
	pos := atomic.AddInt32(&c.pos, 1)
	return c.urls[pos%c.total]
}
