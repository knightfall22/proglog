package main

import (
	"context"
	"fmt"
	"log"

	proglog "github.com/knightfall22/proglog/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// addr := flag.String("addr", ":8400", "service address")
	// flag.Parse()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.NewClient("127.0.0.1:8400", opts...)
	if err != nil {
		log.Fatal(err)
	}
	client := proglog.NewLogClient(conn)
	ctx := context.Background()
	res, err := client.GetServers(ctx, &proglog.GetServersRequest{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("servers:")
	for _, server := range res.Servers {
		fmt.Printf("\t- %v\n", server)
	}

	prodRes, err := client.Produce(ctx, &proglog.ProduceRequest{
		Record: &proglog.Record{
			Value: []byte("I loved long before I knew you"),
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("offset: %v\n", prodRes.Offset)

	conRes, err := client.Consume(ctx, &proglog.ConsumeRequest{
		Offset: 5,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Cosume Response %+v\n", conRes)
}
