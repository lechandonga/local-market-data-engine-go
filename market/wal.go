package market

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
)

// WAL 格式（小端）：
//
//	magic   [4]byte = "MWAL"
//	version uint32  = 1
//	records: 每个 tick 一条
//	  length uint32        // gob payload 字节数
//	  payload [length]byte // gob 编码的 Tick
//
// 只追加、不修改。读取时严格校验帧边界；尾部损坏会返回错误而不是静默截断，
// 避免把已提交数据“改坏”（可用 SyncWrites 保证每条 fsync）。
const (
	walMagic   = "MWAL"
	walVersion = uint32(1)
)

type walWriter struct {
	f    *os.File
	w    *bufio.Writer
	sync bool
}

func openWALWriter(path string, syncWrites bool) (*walWriter, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("market: open wal: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	w := &walWriter{f: f, w: bufio.NewWriter(f), sync: syncWrites}
	if st.Size() == 0 {
		if _, err := f.WriteString(walMagic); err != nil {
			f.Close()
			return nil, err
		}
		if err := binary.Write(f, binary.LittleEndian, walVersion); err != nil {
			f.Close()
			return nil, err
		}
		if syncWrites {
			if err := f.Sync(); err != nil {
				f.Close()
				return nil, err
			}
		}
	}
	return w, nil
}

// Append 追加一条 tick。非 sync 模式下仅保证进程正常退出（Close）后可恢复。
func (w *walWriter) Append(t Tick) error {
	var payload bytes.Buffer
	enc := gob.NewEncoder(&payload)
	if err := enc.Encode(t); err != nil {
		return err
	}
	if err := binary.Write(w.w, binary.LittleEndian, uint32(payload.Len())); err != nil {
		return err
	}
	if _, err := w.w.Write(payload.Bytes()); err != nil {
		return err
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

func (w *walWriter) Close() error {
	if err := w.w.Flush(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

func readWAL(path string) ([]Tick, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("market: read wal: %w", err)
	}
	defer f.Close()

	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return nil, fmt.Errorf("market: wal header: %w", err)
	}
	if string(magic[:]) != walMagic {
		return nil, errors.New("market: bad wal magic")
	}
	var version uint32
	if err := binary.Read(f, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("market: wal version: %w", err)
	}
	if version != walVersion {
		return nil, fmt.Errorf("market: unsupported wal version %d", version)
	}

	var ticks []Tick
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("market: wal frame length: %w", err)
		}
		n := binary.LittleEndian.Uint32(lenBuf[:])
		if n == 0 || n > 64*1024*1024 {
			return nil, fmt.Errorf("market: wal frame size out of range: %d", n)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(f, buf); err != nil {
			return nil, fmt.Errorf("market: wal frame payload: %w", err)
		}
		var t Tick
		if err := gob.NewDecoder(bytes.NewReader(buf)).Decode(&t); err != nil {
			return nil, fmt.Errorf("market: wal decode: %w", err)
		}
		ticks = append(ticks, t)
	}
	return ticks, nil
}
