// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty off
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

// Experimental code for separating data and structural information

import (
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

type TrieBuilder struct {
	key, value []byte // Next key-value pair to consume
	stack      []node
}

func (tb *TrieBuilder) setKeyValue(key, value []byte) {
	tb.key = key
	tb.value = value
}

func (tb *TrieBuilder) branch(digit int) {
	//fmt.Printf("BRANCH %d\n", digit)
	f := &fullNode{}
	f.flags.dirty = true
	n := tb.stack[len(tb.stack)-1]
	f.Children[digit] = n
	tb.stack[len(tb.stack)-1] = f
}

func (tb *TrieBuilder) hasher(digit int) {
	//fmt.Printf("HASHER %d\n", digit)
}

func (tb *TrieBuilder) leaf(length int) {
	//fmt.Printf("LEAF %d\n", length)
	s := &shortNode{Key: hexToCompact(tb.key[len(tb.key)-length:]), Val: valueNode(tb.value)}
	tb.stack = append(tb.stack, s)
}

func (tb *TrieBuilder) extension(key []byte) {
	//fmt.Printf("EXTENSION %x\n", key)
	n := tb.stack[len(tb.stack)-1]
	s := &shortNode{Key: hexToCompact(key), Val: n}
	tb.stack[len(tb.stack)-1] = s
}

func (tb *TrieBuilder) add(digit int) {
	//fmt.Printf("ADD %d\n", digit)
	n := tb.stack[len(tb.stack)-1]
	tb.stack = tb.stack[:len(tb.stack)-1]
	f := tb.stack[len(tb.stack)-1].(*fullNode)
	f.Children[digit] = n
}

func (tb *TrieBuilder) hash() {
	//fmt.Printf("HASH\n")
}

func (tb *TrieBuilder) root() common.Hash {
	if len(tb.stack) == 0 {
		return emptyRoot
	}
	h := newHasher(false)
	defer returnHasherToPool(h)
	var hn common.Hash
	h.hash(tb.stack[len(tb.stack)-1], true, hn[:])
	return hn
}

type HashBuilder struct {
	key, value    []byte // Next key-value pair to consume
	bufferStack   []*bytes.Buffer
	digitStack    []int
	branchStack   []*fullNode
	topKey        []byte
	topValue      []byte
	topHash       common.Hash
	topBranch     *fullNode
	sha           keccakState
	encodeToBytes bool
}

func NewHashBuilder(encodeToBytes bool) *HashBuilder {
	return &HashBuilder{sha: sha3.NewLegacyKeccak256().(keccakState), encodeToBytes: encodeToBytes}
}

func (hb *HashBuilder) setKeyValue(key, value []byte) {
	hb.key = key
	hb.value = value
}

func (hb *HashBuilder) branch(digit int) {
	//fmt.Printf("BRANCH %d\n", digit)
	f := &fullNode{}
	f.flags.dirty = true
	if hb.topKey == nil {
		if len(hb.bufferStack) == 0 {
			n := hb.branchStack[len(hb.branchStack)-1]
			f.Children[digit] = n
			hb.branchStack[len(hb.branchStack)-1] = f
		} else {
			// Finalise existing hasher
			hn, _ := hb.finaliseHasher()
			f.Children[digit] = hashNode(hn[:])
			hb.branchStack = append(hb.branchStack, f)
		}
	} else {
		f.Children[digit] = hb.shortNode()
		hb.topKey = nil
		hb.topValue = nil
		hb.topBranch = nil
		hb.branchStack = append(hb.branchStack, f)
	}
}

func generateStructLen(buffer []byte, l int) int {
	if l < 56 {
		buffer[0] = byte(192 + l)
		return 1
	}
	if l < 256 {
		// l can be encoded as 1 byte
		buffer[1] = byte(l)
		buffer[0] = byte(247 + 1)
		return 2
	}
	if l < 65536 {
		buffer[2] = byte(l & 255)
		buffer[1] = byte(l >> 8)
		buffer[0] = byte(247 + 2)
		return 3
	}
	buffer[3] = byte(l & 255)
	buffer[2] = byte((l >> 8) & 255)
	buffer[1] = byte(l >> 16)
	buffer[0] = byte(247 + 3)
	return 4
}

func (hb *HashBuilder) addLeafToHasher(buffer *bytes.Buffer) error {
	// Compute the total length of binary representation
	var keyPrefix [1]byte
	var valPrefix [4]byte
	var kp, vp, kl, vl int
	// Write key
	compactKey := hexToCompact(hb.topKey)
	if len(compactKey) > 1 || compactKey[0] >= 128 {
		keyPrefix[0] = byte(128 + len(compactKey))
		kp = 1
		kl = len(compactKey)
	} else {
		kl = 1
	}
	var v []byte
	if hb.topValue != nil {
		if len(hb.topValue) > 1 || hb.topValue[0] >= 128 {
			if hb.encodeToBytes {
				// Wrapping into another byte array
				vp = generateByteArrayLenDouble(valPrefix[:], 0, len(hb.topValue))
			} else {
				vp = generateByteArrayLen(valPrefix[:], 0, len(hb.topValue))
			}
			vl = len(hb.topValue)
		} else {
			vl = 1
		}
		v = hb.topValue
	} else if hb.topBranch != nil {
		panic("")
	} else {
		valPrefix[0] = 128 + 32
		vp = 1
		vl = 32
		v = hb.topHash[:]
	}
	totalLen := kp + kl + vp + vl
	if totalLen < 32 {
		// Embedded node
		buffer.Write(keyPrefix[:kp])
		buffer.Write(compactKey)
		buffer.Write(valPrefix[:vp])
		buffer.Write(v)
	} else {
		var lenPrefix [4]byte
		pt := generateStructLen(lenPrefix[:], totalLen)
		hb.sha.Reset()
		if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
			return err
		}
		if _, err := hb.sha.Write(keyPrefix[:kp]); err != nil {
			return err
		}
		if _, err := hb.sha.Write(compactKey); err != nil {
			return err
		}
		if _, err := hb.sha.Write(valPrefix[:vp]); err != nil {
			return err
		}
		if _, err := hb.sha.Write(v); err != nil {
			return err
		}
		var hn common.Hash
		if _, err := hb.sha.Read(hn[:]); err != nil {
			return err
		}
		if err := buffer.WriteByte(128 + 32); err != nil {
			return err
		}
		if _, err := buffer.Write(hn[:]); err != nil {
			return err
		}
	}
	return nil
}

func (hb *HashBuilder) finaliseHasher() (common.Hash, error) {
	prevDigit := hb.digitStack[len(hb.digitStack)-1]
	hb.digitStack = hb.digitStack[:len(hb.digitStack)-1]
	prevBuffer := hb.bufferStack[len(hb.bufferStack)-1]
	hb.bufferStack = hb.bufferStack[:len(hb.bufferStack)-1]
	for i := prevDigit + 1; i < 17; i++ {
		prevBuffer.WriteByte(128)
	}
	var lenPrefix [4]byte
	pt := generateStructLen(lenPrefix[:], prevBuffer.Len())
	hb.sha.Reset()
	if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
		return common.Hash{}, err
	}
	if _, err := hb.sha.Write(prevBuffer.Bytes()); err != nil {
		return common.Hash{}, err
	}
	var hn common.Hash
	if _, err := hb.sha.Read(hn[:]); err != nil {
		return common.Hash{}, err
	}
	return hn, nil
}

func (hb *HashBuilder) hasher(digit int) {
	//fmt.Printf("HASHER %d\n", digit)
	var buffer bytes.Buffer
	for i := 0; i < digit; i++ {
		buffer.WriteByte(128) // Empty array
	}
	if hb.topKey == nil {
		if len(hb.bufferStack) == 0 {
			panic("")
		} else {
			hn, _ := hb.finaliseHasher()
			buffer.WriteByte(128 + 32)
			buffer.Write(hn[:])
		}
	} else {
		if err := hb.addLeafToHasher(&buffer); err != nil {
			panic(err)
		}
	}
	hb.bufferStack = append(hb.bufferStack, &buffer)
	hb.topKey = nil
	hb.topValue = nil
	hb.topBranch = nil
	hb.digitStack = append(hb.digitStack, digit)
}

func (hb *HashBuilder) leaf(length int) {
	//fmt.Printf("LEAF %d\n", length)
	hb.topKey = hb.key[len(hb.key)-length:]
	hb.topValue = hb.value
	hb.topBranch = nil
}

func (hb *HashBuilder) extension(key []byte) {
	//fmt.Printf("EXTENSION %x\n", key)
	if len(hb.bufferStack) == 0 {
		f := hb.branchStack[len(hb.branchStack)-1]
		hb.branchStack = hb.branchStack[:len(hb.branchStack)-1]
		hb.topBranch = f
	} else {
		hn, _ := hb.finaliseHasher()
		hb.topHash = hn
		hb.topBranch = nil
	}
	hb.topKey = key
	hb.topValue = nil
}

func (hb *HashBuilder) shortNode() *shortNode {
	if hb.topValue != nil {
		return &shortNode{Key: hexToCompact(hb.topKey), Val: valueNode(hb.topValue)}
	} else if hb.topBranch != nil {
		return &shortNode{Key: hexToCompact(hb.topKey), Val: hb.topBranch}
	}
	return &shortNode{Key: hexToCompact(hb.topKey), Val: hashNode(common.CopyBytes(hb.topHash[:]))}
}

func (hb *HashBuilder) add(digit int) {
	//fmt.Printf("ADD %d\n", digit)
	if len(hb.bufferStack) == 0 {
		f := hb.branchStack[len(hb.branchStack)-1]
		if hb.topKey == nil {
			n := f
			hb.branchStack = hb.branchStack[:len(hb.branchStack)-1]
			f = hb.branchStack[len(hb.branchStack)-1]
			f.Children[digit] = n
		} else {
			f.Children[digit] = hb.shortNode()
			hb.topKey = nil
			hb.topValue = nil
			hb.topBranch = nil
		}
	} else {
		prevBuffer := hb.bufferStack[len(hb.bufferStack)-1]
		prevDigit := hb.digitStack[len(hb.digitStack)-1]
		if hb.topKey == nil {
			hn, _ := hb.finaliseHasher()
			if len(hb.bufferStack) > 0 {
				prevBuffer = hb.bufferStack[len(hb.bufferStack)-1]
				prevDigit = hb.digitStack[len(hb.digitStack)-1]
				for i := prevDigit + 1; i < digit; i++ {
					prevBuffer.WriteByte(128)
				}
				prevBuffer.WriteByte(128 + 32)
				prevBuffer.Write(hn[:])
				hb.digitStack[len(hb.digitStack)-1] = digit
			} else {
				f := hb.branchStack[len(hb.branchStack)-1]
				f.Children[digit] = hashNode(hn[:])
			}
		} else {
			for i := prevDigit + 1; i < digit; i++ {
				prevBuffer.WriteByte(128)
			}
			if err := hb.addLeafToHasher(prevBuffer); err != nil {
				panic(err)
			}
			hb.digitStack[len(hb.digitStack)-1] = digit
			hb.topKey = nil
			hb.topValue = nil
			hb.topBranch = nil
		}
	}
}

func (hb *HashBuilder) hash() {
	//fmt.Printf("HASH\n")
}

func (hb *HashBuilder) root() node {
	if hb.topKey == nil {
		if len(hb.bufferStack) == 0 {
			if len(hb.branchStack) == 0 {
				return nil
			}
			return hb.branchStack[len(hb.branchStack)-1]
		}
		hn, _ := hb.finaliseHasher()
		return hashNode(hn[:])
	}
	return hb.shortNode()
}

func (hb *HashBuilder) rootHash() common.Hash {
	var hn common.Hash
	if hb.topKey == nil {
		if len(hb.bufferStack) == 0 {
			if len(hb.branchStack) == 0 {
				return emptyRoot
			}
			h := newHasher(hb.encodeToBytes)
			defer returnHasherToPool(h)
			h.hash(hb.branchStack[len(hb.branchStack)-1], true, hn[:])
		} else {
			hn, _ = hb.finaliseHasher()
			return hn
		}
	} else {
		h := newHasher(hb.encodeToBytes)
		defer returnHasherToPool(h)
		h.hash(hb.shortNode(), true, hn[:])
	}
	return hn
}

func TestTrieBuilding(t *testing.T) {
	var keys []string
	for b := uint32(0); b < 10000; b++ {
		var preimage [4]byte
		binary.BigEndian.PutUint32(preimage[:], b)
		key := keybytesToHex(crypto.Keccak256(preimage[:])[:4])
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	tr := New(common.Hash{}, false)
	value := []byte("VALUE123985903485903489043859043859043859048590485904385903485940385439058934058439058439058439058940385904358904385438809348908345")
	for _, key := range keys {
		_, tr.root = tr.insert(tr.root, []byte(key), 0, valueNode(value), 0)
	}
	trieHash := tr.Hash()

	var tb TrieBuilder
	var prec, curr, succ []byte
	groups := make(map[string]uint32)
	for _, key := range keys {
		prec = curr
		curr = succ
		succ = []byte(key)
		if curr != nil {
			step(func(prefix []byte) bool { return false }, false, prec, curr, succ, &tb, groups)
		}
		tb.setKeyValue([]byte(key), value)
	}
	prec = curr
	curr = succ
	succ = nil
	step(func(prefix []byte) bool { return false }, false, prec, curr, succ, &tb, groups)
	builtHash := tb.root()
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
}

func TestHashBuilding(t *testing.T) {
	var keys []string
	for b := uint32(0); b < 10000; b++ {
		var preimage [4]byte
		binary.BigEndian.PutUint32(preimage[:], b)
		key := keybytesToHex(crypto.Keccak256(preimage[:])[:4])
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	tr := New(common.Hash{}, false)
	value := []byte("VALUE123985903485903489043859043859043859048590485904385903485940385439058934058439058439058439058940385904358904385438809348908345")
	for _, key := range keys {
		_, tr.root = tr.insert(tr.root, []byte(key), 0, valueNode(value), 0)
	}
	trieHash := tr.Hash()

	hb := NewHashBuilder(false)
	var prec, curr, succ []byte
	groups := make(map[string]uint32)
	for _, key := range keys {
		prec = curr
		curr = succ
		succ = []byte(key)
		if curr != nil {
			step(func(prefix []byte) bool { return true }, false, prec, curr, succ, hb, groups)
		}
		hb.setKeyValue([]byte(key), value)
	}
	prec = curr
	curr = succ
	succ = nil
	step(func(prefix []byte) bool { return true }, false, prec, curr, succ, hb, groups)
	builtHash := hb.rootHash()
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
}

func TestResolution(t *testing.T) {
	var keys []string
	for b := uint32(0); b < 10000; b++ {
		var preimage [4]byte
		binary.BigEndian.PutUint32(preimage[:], b)
		key := keybytesToHex(crypto.Keccak256(preimage[:])[:4])
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	tr := New(common.Hash{}, false)
	value := []byte("VALUE123985903485903489043859043859043859048590485904385903485940385439058934058439058439058439058940385904358904385438809348908345")
	for _, key := range keys {
		_, tr.root = tr.insert(tr.root, []byte(key), 0, valueNode(value), 0)
	}
	trieHash := tr.Hash()

	// Choose some keys to be resolved
	var rs ResolveSet
	// First, existing keys
	for i := 0; i < 1000; i += 200 {
		rs.AddKey([]byte(keys[i]))
	}
	// Next, some non-exsiting keys
	for i := 0; i < 100; i++ {
		key := keybytesToHex(crypto.Keccak256([]byte(keys[i]))[:4])
		rs.AddKey(key)
	}

	hb := NewHashBuilder(false)
	var prec, curr, succ []byte
	groups := make(map[string]uint32)
	for _, key := range keys {
		prec = curr
		curr = succ
		succ = []byte(key)
		if curr != nil {
			step(rs.HashOnly, false, prec, curr, succ, hb, groups)
		}
		hb.setKeyValue([]byte(key), value)
	}
	prec = curr
	curr = succ
	succ = nil
	step(rs.HashOnly, false, prec, curr, succ, hb, groups)
	tr1 := New(common.Hash{}, false)
	tr1.root = hb.root()
	builtHash := hb.rootHash()
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
	// Check the availibility of the resolved keys
	for _, hex := range rs.keys {
		key := hexToKeybytes(hex)
		_, found := tr1.Get(key, 0)
		if !found {
			t.Errorf("Key %x was not resolved", hex)
		}
	}
}
