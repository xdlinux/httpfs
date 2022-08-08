package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/winfsp/cgofuse/fuse"
	gcfg "gopkg.in/gcfg.v1"

	httpfs "frankli.site/httpfs/internal"
)

type Config struct {
	Profile map[string]*httpfs.Profile
}

func main() {
	config := &Config{}
	conf := flag.String("conf", "./httpfs.conf", "config file location")
	prof := flag.String("prof", "", "profile")
	flag.Parse()

	gcfg.ReadFileInto(config, *conf)
	profile, ok := config.Profile[*prof]
	if !ok {
		fmt.Println("Invalid Profile")
		return
	}
	host := fuse.NewFileSystemHost(httpfs.NewHttpFs(profile))
	go func() {
		chan_signal := make(chan os.Signal, 1)
		signal.Notify(
			chan_signal,
			syscall.SIGINT,
			syscall.SIGTERM,
		)
		<-chan_signal
		host.Unmount()
	}()
	host.Mount(profile.MountPoint, nil)
}
