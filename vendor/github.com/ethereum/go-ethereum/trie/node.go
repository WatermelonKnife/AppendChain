// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"fmt"
	"io"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"math/big"

)

var indices = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "[17]"}

type node interface {
	fstring(string) string
	cache() (hashNode, bool)
}

type (
	fullNode struct {
		Children [17]node // Actual trie node data to encode/decode (needs custom encoder)
		flags    nodeFlag
	}
	shortNode struct {
		Key   []byte
		Val   node
		flags nodeFlag
	}
	hashNode  []byte
	
	valueNode []byte

)

// nilValueNode is used when collapsing internal trie nodes for hashing, since
// unset children need to serialize correctly.
var nilValueNode = valueNode(nil)

// EncodeRLP encodes a full node into the consensus RLP format.
func (n *fullNode) EncodeRLP(w io.Writer) error {
	var nodes [17]node

	for i, child := range &n.Children {
		if child != nil {
			nodes[i] = child
		} else {
			nodes[i] = nilValueNode
		}
	}
	return rlp.Encode(w, nodes)
}

func (n *fullNode) copy() *fullNode   { copy := *n; return &copy }
func (n *shortNode) copy() *shortNode { copy := *n; return &copy }

// nodeFlag contains caching-related metadata about a node.
type nodeFlag struct {
	hash  hashNode // cached hash of the node (may be nil)
	dirty bool     // whether the node has changes that must be written to the database
}

func (n *fullNode) cache() (hashNode, bool)  { return n.flags.hash, n.flags.dirty }
func (n *shortNode) cache() (hashNode, bool) { return n.flags.hash, n.flags.dirty }
func (n hashNode) cache() (hashNode, bool)   { return nil, true }
func (n valueNode) cache() (hashNode, bool)  { return nil, true }

// Pretty printing.
func (n *fullNode) String() string  { return n.fstring("") }
func (n *shortNode) String() string { return n.fstring("") }
func (n hashNode) String() string   { return n.fstring("") }
func (n valueNode) String() string  { return n.fstring("") }

func (n *fullNode) fstring(ind string) string {
	resp := fmt.Sprintf("[\n%s  ", ind)
	for i, node := range &n.Children {
		if node == nil {
			resp += fmt.Sprintf("%s: <nil> ", indices[i])
		} else {
			resp += fmt.Sprintf("%s: %v", indices[i], node.fstring(ind+"  "))
		}
	}
	return resp + fmt.Sprintf("\n%s] ", ind)
}
func (n *shortNode) fstring(ind string) string {
	return fmt.Sprintf("{%x: %v} ", n.Key, n.Val.fstring(ind+"  "))
}
func (n hashNode) fstring(ind string) string {
	return fmt.Sprintf("<%x> ", []byte(n))
}
func (n valueNode) fstring(ind string) string {
	return fmt.Sprintf("%x ", []byte(n))
}

func mustDecodeNode(hash, buf []byte) node {
	n, err := decodeNode(hash, buf)
	if err != nil {
		return nil
		//panic(fmt.Sprintf("node %x: %v", hash, err))
	}
	return n
}

// decodeNode parses the RLP encoding of a trie node.
func decodeNode(hash, buf []byte) (node, error) {
	if len(buf) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	elems, _, err := rlp.SplitList(buf)
	if err != nil {
		return nil, fmt.Errorf("decode error: %v", err)
	}
	switch c, _ := rlp.CountValues(elems); c {
	case 2:
		n, err := decodeShort(hash, elems)
		return n, wrapError(err, "short")
	case 17:
		n, err := decodeFull(hash, elems)
		return n, wrapError(err, "full")
	default:
		return nil, fmt.Errorf("invalid number of list elements: %v", c)
	}
}

func decodeShort(hash, elems []byte) (node, error) {
	kbuf, rest, err := rlp.SplitString(elems)
	if err != nil {
		return nil, err
	}
	flag := nodeFlag{hash: hash}
	key := compactToHex(kbuf)
	if hasTerm(key) {
		// value node
		val, _, err := rlp.SplitString(rest)
		if err != nil {
			return nil, fmt.Errorf("invalid value node: %v", err)
		}
		return &shortNode{key, append(valueNode{}, val...), flag}, nil
	}
	r, _, err := decodeRef(rest)
	if err != nil {
		return nil, wrapError(err, "val")
	}
	return &shortNode{key, r, flag}, nil
}

func decodeFull(hash, elems []byte) (*fullNode, error) {
	n := &fullNode{flags: nodeFlag{hash: hash}}
	for i := 0; i < 16; i++ {
		cld, rest, err := decodeRef(elems)
		if err != nil {
			return n, wrapError(err, fmt.Sprintf("[%d]", i))
		}
		n.Children[i], elems = cld, rest
	}
	val, _, err := rlp.SplitString(elems)
	if err != nil {
		return n, err
	}
	if len(val) > 0 {
		n.Children[16] = append(valueNode{}, val...)
	}
	return n, nil
}

const hashLen = len(common.Hash{})

func decodeRef(buf []byte) (node, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, buf, err
	}
	switch {
	case kind == rlp.List:
		// 'embedded' node reference. The encoding must be smaller
		// than a hash in order to be valid.
		if size := len(buf) - len(rest); size > hashLen {
			err := fmt.Errorf("oversized embedded node (size is %d bytes, want size < %d)", size, hashLen)
			return nil, buf, err
		}
		n, err := decodeNode(nil, buf)
		return n, rest, err
	case kind == rlp.String && len(val) == 0:
		// empty node
		return nil, rest, nil
	case kind == rlp.String && len(val) == 32:
		return append(hashNode{}, val...), rest, nil
	default:
		return nil, nil, fmt.Errorf("invalid RLP string size %d (want 0 or 32)", len(val))
	}
}

func mustDecodeNode2(key []byte, buf []byte) node {
	n, err := decodeNode2(key, buf)
	if err != nil {
		return nil
		//panic(fmt.Sprintf("node %x: %v", key, err))
	}
	return n
}

// decodeNode2 parses the RLP encoding of a trie node.
func decodeNode2(key []byte, buf []byte) (node, error) {
	if len(buf) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	elems, _, err := rlp.SplitList(buf)
	if err != nil {
		return nil, fmt.Errorf("decode error: %v", err)
	}
	switch c, _ := rlp.CountValues(elems); c {
	case 2:
		n, err := decodeShort2(key, elems)
		return n, wrapError(err, "short")
	case 17:
		n, err := decodeFull2(key, elems)
		return n, wrapError(err, "full")
	default:
		return nil, fmt.Errorf("invalid number of list elements: %v", c)
	}
}

func decodeShort2(key, elems []byte) (node, error) {
	kbuf, rest, err := rlp.SplitString(elems)
	if err != nil { return nil, err }
	flag := nodeFlag{hash: key[:]}
	compactKey := compactToHex(kbuf)
	if hasTerm(compactKey) {
		// value node
		val, _, err := rlp.SplitString(rest)
		if err != nil { 
			return nil, fmt.Errorf("invalid value node: %v", err) 
		}
		return &shortNode{compactKey, append(valueNode{}, val...), flag}, nil
	}
	r, _, err := decodeRef2(rest)
	if err != nil { 
		return nil, wrapError(err, "val") 
	}
	return &shortNode{compactKey, r, flag}, nil
}

func decodeFull2(key, elems []byte) (*fullNode, error) {
	n := &fullNode{flags: nodeFlag{hash: key[:]}}
	for i := 0; i < 17; i++ {
		cld, rest, err := decodeRef2(elems)
		if err != nil {
			return n, wrapError(err, fmt.Sprintf("[%d]", i))
		}
		n.Children[i], elems = cld, rest
	}
	//val, _, err := rlp.SplitString(elems)
	//if err != nil {
	//	return n, err
	//}
	//if len(val) > 0 {
	//	n.Children[16] = append(valueNode{}, val...)
	//}
	return n, nil
}

//const hashLen = len(common.Hash{})

func decodeRef2(buf []byte) (node, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, buf, err
	}
	switch {
	case kind == rlp.List && len(val) == 0:
		// empty node
		return nil, rest, nil
		
	case kind == rlp.List && len(val) < 43:
		newVal := append([]byte{buf[0]}, val...)
		newKey, err := decodeNewKey(newVal)
		if err != nil {
			fmt.Printf("Failed to decode NewKey: %v\n", err)
			return nil, rest, nil
		}
		return newKey, rest, nil
	case kind == rlp.List:
		// 'embedded' node reference. The encoding must be smaller
		// than a hash in order to be valid.
		if size := len(buf) - len(rest); size > 42 {
			err := fmt.Errorf("oversized embedded node (size is %d bytes, want size < %d)", size, 42)
			return nil, buf, err
		}
		n, err := decodeNode2(nil, buf)
		return n, rest, err
	case kind == rlp.String && len(val) == 0:
		// empty node
		return nil, rest, nil
		
	case kind == rlp.String && len(val) == 32:
		return append(hashNode{}, val...), rest, nil

	default:
		return nil, nil, fmt.Errorf("invalid RLP string size %d (want 0 or 42)", len(val))
	}
}



func decodeNewKey(encoded []byte) (*NewKey, error) {
	var decoded []interface{}
	if err := rlp.DecodeBytes(encoded, &decoded); err != nil {
		return nil, err
	}
	if len(decoded) != 4 {
		return nil, fmt.Errorf("invalid RLP list size %d (want 4)", len(decoded))
	}

	numBytes, ok := decoded[0].([]uint8)
	if !ok {
		return nil, fmt.Errorf("invalid type for Num field")
	}
	var num uint64
	if len(numBytes) == 0 {
		num = 0
	} else {
		num = new(big.Int).SetBytes(numBytes).Uint64()
	}

	depthBytes, ok := decoded[1].([]uint8)
	if !ok {
		return nil, fmt.Errorf("invalid type for Depth field")
	}
	var depth byte
	if len(depthBytes) == 0 {
		depth = 0
	} else {
		depth = depthBytes[0]
	}

	nodeIndexBytes, ok := decoded[2].([]uint8)
	if !ok {
		return nil, fmt.Errorf("invalid type for NodeIndex field")
	}
	var nodeIndex byte
	if len(nodeIndexBytes) == 0 {
		nodeIndex = 0
	} else {
		nodeIndex = nodeIndexBytes[0]
	}

	hash, ok := decoded[3].([]uint8)
	if !ok || len(hash) != 32 {
		return nil, fmt.Errorf("invalid type or length for Hash field")
	}
	newKey := &NewKey{
		Num:       num,
		Depth:     depth,
		NodeIndex: nodeIndex,
		Hash:      common.BytesToHash(hash),
	}
	
	return newKey, nil
}

// wraps a decoding error with information about the path to the
// invalid child node (for debugging encoding issues).
type decodeError struct {
	what  error
	stack []string
}

func wrapError(err error, ctx string) error {
	if err == nil {
		return nil
	}
	if decErr, ok := err.(*decodeError); ok {
		decErr.stack = append(decErr.stack, ctx)
		return decErr
	}
	return &decodeError{err, []string{ctx}}
}

func (err *decodeError) Error() string {
	return fmt.Sprintf("%v (decode path: %s)", err.what, strings.Join(err.stack, "<-"))
}
