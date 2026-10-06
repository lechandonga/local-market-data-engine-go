package engine

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

// WAL 是只追加的本地 tick 日志。只持久化“已生效”的 tick，
// 因此重启回放天然跳过重复/被拒绝数据，产出与重启前一致。
//
// 记录格式（小端）：
//   魔数 0x544B ("TK") 1 字节? 这里用 2 字节: 'T','K'
//   symbolLen u16 | symbol utf8
//   time u64 | price u64 | volume u64

const (
	walMagic0 = 'T'
	walMagic1 = 'K'
)

type WAL struct {
	mu     sync.Mutex
	f      *os.File
	w      *bufio.Writer
	sync   bool // 每条记录是否 fsync
	closed bool
}

// OpenWAL 打开（不存在则创建）WAL。appendOnly=true 时保留旧内容用于回放。
func OpenWAL(path string, fsyncEach bool) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, w: bufio.NewWriter(f), sync: fsyncEach}, nil
}

// Append 追加一条已生效 tick。
func (l *WAL) Append(t Tick) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("engine: wal closed")
	}
	buf := make([]byte, 0, len(t.Symbol)+26)
	buf = append(buf, walMagic0, walMagic1)
	var lb [2]byte
	binary.LittleEndian.PutUint16(lb[:], uint16(len(t.Symbol)))
	buf = append(buf, lb[:]...)
	buf = append(buf, t.Symbol...)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], uint64(t.Time))
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint64(tmp[:], t.Price)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint64(tmp[:], t.Volume)
	buf = append(buf, tmp[:]...)
	if _, err := l.w.Write(buf); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	if l.sync {
		return l.f.Sync()
	}
	return nil
}

// Close 刷盘并关闭文件。
func (l *WAL) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if err := l.w.Flush(); err != nil {
		l.f.Close()
		return err
	}
	return l.f.Close()
}

// ReplayWAL 从头读取 WAL，对每条记录调用 fn。读取到文件结尾正常结束；
// 遇到损坏记录（如进程崩溃导致的半条写入）会在同步刷盘策略下不出现，
// 非同步模式下返回 io.ErrUnexpectedEOF 之前的记录均已重放。
func ReplayWAL(path string, fn func(Tick) error) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	n := 0
	for {
		m0, err := r.ReadByte()
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		m1, err := r.ReadByte()
		if err != nil {
			return n, io.ErrUnexpectedEOF
		}
		if m0 != walMagic0 || m1 != walMagic1 {
			return n, errors.New("engine: corrupt wal magic")
		}
		var lb [2]byte
		if _, err := io.ReadFull(r, lb[:]); err != nil {
			return n, io.ErrUnexpectedEOF
		}
		symLen := int(binary.LittleEndian.Uint16(lb[:]))
		sb := make([]byte, symLen)
		if _, err := io.ReadFull(r, sb); err != nil {
			return n, io.ErrUnexpectedEOF
		}
		var tb [24]byte
		if _, err := io.ReadFull(r, tb[:]); err != nil {
			return n, io.ErrUnexpectedEOF
		}
		t := Tick{
			Symbol: string(sb),
			Time:   int64(binary.LittleEndian.Uint64(tb[0:8])),
			Price:  binary.LittleEndian.Uint64(tb[8:16]),
			Volume: binary.LittleEndian.Uint64(tb[16:24]),
		}
		if err := fn(t); err != nil {
			return n, err
		}
		n++
	}
}
