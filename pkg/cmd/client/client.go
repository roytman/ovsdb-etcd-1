package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"

	"github.com/roytman/jrpc2"
	"github.com/roytman/jrpc2/channel"
)

var serverAddr = flag.String("server", "", "Server address")

func list_dbs(ctx context.Context, cli *jrpc2.Client) (result []string, err error) {
	err = cli.CallResult(ctx, "list_dbs", nil, &result)
	return
}

func echo(ctx context.Context, cli *jrpc2.Client) (result []string, err error) {
	err = cli.CallResult(ctx, "echo", []string{"ech0"}, &result)
	return
}

func main() {
	flag.Parse()
	if *serverAddr == "" {
		log.Fatal("You must provide -server address to connect to")
	}

	conn, err := net.Dial(jrpc2.Network(*serverAddr), *serverAddr)
	if err != nil {
		log.Fatalf("Dial %q: %v", *serverAddr, err)
	}
	log.Printf("Connected to %v", conn.RemoteAddr())

	// Start up the client, and enable logging to stderr.
	cli := jrpc2.NewClient(channel.RawJSON(conn, conn), &jrpc2.ClientOptions{
		OnNotify: func(req *jrpc2.Request) {
			var params json.RawMessage
			req.UnmarshalParams(&params)
			log.Printf("[server push] Method %q params %#q", req.Method(), string(params))
		},
		AllowV1: true,
	})
	defer cli.Close()
	ctx := context.Background()

	log.Print("\n-- Sending some individual requests...")

	if dbs, err := list_dbs(ctx, cli); err != nil {
		log.Fatalln("Ovsdb.List_dbs:", err)
	} else {
		log.Printf("Ovsdb.List_dbs result=%v", dbs)
	}
	if dbs, err := echo(ctx, cli); err != nil {
		log.Fatalln("Ovsdb.Echo:", err)
	} else {
		log.Printf("Ovsdb.Echo result=%v", dbs)
	}
}
