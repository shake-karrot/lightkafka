package broker

import (
	"fmt"
	"io"
	"lightkafka/internal/partition"
	"lightkafka/internal/protocol"
	"net"
	"sync"
)

type Broker struct {
	Config    Config
	Partition *partition.Partition //TODO(Danu): 파티션 관리 추가

	quit chan struct{}
	wg   sync.WaitGroup
}

func NewBroker(cfg Config, p *partition.Partition) *Broker {
	return &Broker{
		Config:    cfg,
		Partition: p,
		quit:      make(chan struct{}),
	}
}

func (b *Broker) Start() error {

	ln, err := net.Listen("tcp", b.Config.ListenAddr)
	if err != nil {
		return err
	}

	fmt.Printf("[Broker] Listening on %s\n", b.Config.ListenAddr)

	go func() {
		<-b.quit
		fmt.Println("[Broker] Stopping... closing listener")
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-b.quit:
				return nil
			default:
				fmt.Printf("[Broker] Accept error: %v\n", err)
				continue
			}
		}

		b.wg.Add(1)
		go b.handleConnection(conn)
	}
}

func (b *Broker) Stop() {
	close(b.quit)
	b.wg.Wait()
}

func (b *Broker) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		b.wg.Done()
	}()

	for {
		req, err := protocol.ReadRequest(conn)
		if err != nil {
			if err != io.EOF {
				fmt.Printf("[Broker] Connection closed/error: %v\n", err)
			}
			return
		}

		err = func() error {

			// NOTE(Danu): 요청 처리 후 메모리 반납
			defer req.Release()

			respBody, handleErr := b.handleRequest(req)
			if handleErr != nil {
				fmt.Printf("[Broker] Handler Error: %v\n", handleErr)
				return handleErr
			}

			return protocol.SendResponse(conn, req.Header.CorrelationID, respBody)
		}()

		if err != nil {
			return
		}
	}
}
