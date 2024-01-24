package main

import (
	"github.com/892294101/mparallel/src/bus"
	"net/http"
	_ "net/http/pprof"
)

func main() {

	go func() {
		_ = http.ListenAndServe("0.0.0.0:29378", nil)
	}()

	b := bus.NewBus()

	defer b.Close()
	if err := b.LoadMParallel(); err != nil {
		b.Log(err)
	} else {
		if err := b.Relocate(); err != nil {
			b.Log(err)
		}
	}

}
