package main

import (
	"application/node"
)

func NewNode(addr string) dhtNode {
	node := new(node.Node)
	node.Init(addr)
	return node
}
