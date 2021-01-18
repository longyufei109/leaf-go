package http

import (
	"encoding/json"
	"github.com/longyufei109/leaf-go/config"
	"github.com/longyufei109/leaf-go/service"
	"log"
	stdhttp "net/http"
)

var svc service.IdGenerator

func Start(g service.IdGenerator) {
	svc = g

	mux := stdhttp.NewServeMux()
	mux.HandleFunc(config.Global.Http.RequestPath, genId)

	server := stdhttp.Server{
		Addr:    config.Global.Http.Addr,
		Handler: mux,
	}
	log.Print("HTTP Server start at [%s]", config.Global.Http.Addr)
	log.Print("", server.ListenAndServe())
}

type response struct {
	Id  int64  `json:"id"`
	Msg string `json:"msg"`
}

func genId(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	key := r.URL.Query().Get(config.Global.Http.Query)
	id, err := svc.Gen(key)
	resp := &response{
		Id: id,
	}
	if err != nil {
		log.Print("genId failed, err:%v", err)
		resp.Msg = err.Error()
		w.WriteHeader(stdhttp.StatusInternalServerError)
	} else {
		w.WriteHeader(stdhttp.StatusOK)
	}
	data, _ := json.Marshal(resp)
	_, _ = w.Write(data)
}
