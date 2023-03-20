package main

import (
	"fmt"
	"net"

	"github.com/ccfarm/simple_cache/handler"
)

func main() {
	// 1.启动服务
	listen, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Printf("listen failed,err:%v\n", err)
		return
	}

	// 2.等待客户端来建立连接
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Printf("listen failed,err:%v\n", err)
			continue
		}

		// 3.启动单独的goroutine去处理连接
		go handler.Handle(conn)
	}
}
