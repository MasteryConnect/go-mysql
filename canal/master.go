package canal

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
)

type masterInfo struct {
	Addr     string `toml:"addr"`
	Name     string `toml:"bin_name"`
	Position uint32 `toml:"bin_pos"`

	name string

	l sync.Mutex

	lastSaveTime time.Time

	infoLoader MasterInfoLoader
}

// abstract the way in which the master info is loaded and saved
type MasterInfoSetter func(addr, name string, position uint32) error
type MasterInfoLoader interface {
	Load(setValues MasterInfoSetter) error
	Save(addr, name string, position uint32, force bool) error
}

func (m *masterInfo) Setter(addr, name string, position uint32) error {
	m.Addr = addr
	m.Name = name
	m.Position = position
	return nil
}

func (m *masterInfo) Save(force bool) error {
	m.l.Lock()
	defer m.l.Unlock()

	n := time.Now()
	if !force && n.Sub(m.lastSaveTime) < time.Second {
		return nil
	}

	err := m.infoLoader.Save(m.Addr, m.Name, m.Position, force)

	m.lastSaveTime = n

	return errors.Trace(err)
}

func (m *masterInfo) Update(name string, pos uint32) {
	m.l.Lock()
	m.Name = name
	m.Position = pos
	m.l.Unlock()
}

func (m *masterInfo) Pos() mysql.Position {
	var pos mysql.Position
	m.l.Lock()
	pos.Name = m.Name
	pos.Pos = m.Position
	m.l.Unlock()

	return pos
}

func (m *masterInfo) Close() {
	m.Save(true)
}
