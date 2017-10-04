package sandglass

import (
	"fmt"

	"github.com/celrenheit/sandglass/sgproto"
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
}

func (n *Node) Dial() (err error) {
	n.conn, err = grpc.Dial(n.GRPCAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	fmt.Println("dialing", n.Name)
	n.BrokerServiceClient = sgproto.NewBrokerServiceClient(n.conn)

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
