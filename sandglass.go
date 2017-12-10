package sandglass

import (
	"fmt"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/hashicorp/serf/serf"
	"google.golang.org/grpc"
)

type Node struct {
	ID       string
	Name     string
	IP       string
	GRPCAddr string
	RAFTAddr string
	HTTPAddr string
	Status   serf.MemberStatus
	conn     *grpc.ClientConn
	sgproto.BrokerServiceClient
	sgproto.InternalServiceClient
}

func (n *Node) Dial() (err error) {
	n.conn, err = grpc.Dial(n.GRPCAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	n.BrokerServiceClient = sgproto.NewBrokerServiceClient(n.conn)
	n.InternalServiceClient = sgproto.NewInternalServiceClient(n.conn)

	return nil
}

func (n *Node) Close() error {
	if n.conn == nil {
		return nil
	}
	if err := n.conn.Close(); err != nil {
		return err
	}
	n.conn = nil
	return nil
}

func (n *Node) String() string {
	return fmt.Sprintf("%s(%s)", n.Name, n.GRPCAddr)
}

func (n *Node) IsAlive() bool {
	return n.conn != nil
}
