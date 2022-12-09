package rawmedia

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/H0RlZ0N/joy4/av"
	"github.com/H0RlZ0N/joy4/codec/aacparser"
	"github.com/H0RlZ0N/joy4/codec/h264parser"
	"github.com/H0RlZ0N/joy4/utils/bits/pio"
)

type Packet struct {
	Datatype DataType
	Data     []byte
	Pts      uint64 // Microsecond
}

type Stream struct {
	av.CodecData
	demuxer *Demuxer

	StreamType DataType
	Idx        int
	BReady     bool
}

type Demuxer struct {
	RwLocker sync.RWMutex
	pkts     []*av.Packet
	streams  []*Stream
	stage    int
}

type MediaDir int32

const (
	MEDIADIR_UNKNOWN   MediaDir = 0
	MEDIADIR_VIDEO     MediaDir = 1
	MEDIADIR_AUDIOSEND MediaDir = 4
)

type DataType int32

const (
	StreamTypeH264    DataType = 0
	StreamTypeAdtsAAC DataType = 1
)

func NewDemuxer() *Demuxer {
	return &Demuxer{}
}

func (self *Demuxer) Streams() (streams []av.CodecData, err error) {
	for _, stream := range self.streams {
		if stream.CodecData != nil {
			streams = append(streams, stream.CodecData)
		}
	}

	if len(streams) == 0 {
		err = fmt.Errorf("no stream found")
	}

	return
}

func (self *Demuxer) GetStreams() []*Stream {
	return self.streams
}

func (self *Demuxer) FillStreams() {
	self.AddVideoStreams()
	self.AddAudioStreams()
}

func (self *Demuxer) AddVideoStreams() {
	stream := &Stream{}
	stream.Idx = 0
	stream.demuxer = self
	stream.StreamType = StreamTypeH264
	stream.BReady = false
	self.streams = append(self.streams, stream)
	return
}

func (self *Demuxer) AddAudioStreams() {
	stream := &Stream{}
	stream.Idx = 1
	stream.demuxer = self
	stream.StreamType = StreamTypeAdtsAAC
	stream.BReady = false
	self.streams = append(self.streams, stream)
	return
}

func (self *Demuxer) ReadPacket() (pkt av.Packet, err error) {
	self.RwLocker.Lock()
	defer self.RwLocker.Unlock()
	if len(self.pkts) == 0 {
		return pkt, fmt.Errorf("EOF packet")
	}
	pkt = *self.pkts[0]
	self.pkts = self.pkts[1:]
	return
}

var timeStamp time.Duration = 0

func (self *Stream) addPacket(payload []byte, iskeyframe bool, pts uint64) {
	pkt := av.Packet{
		Idx:        int8(self.Idx),
		IsKeyFrame: iskeyframe,
		Time:       time.Duration(pts) * time.Microsecond,
		Data:       payload,
	}
	self.demuxer.RwLocker.Lock()
	defer self.demuxer.RwLocker.Unlock()
	self.demuxer.pkts = append(self.demuxer.pkts, &pkt)
}

func (self *Stream) PackMediaData(payload []byte, pts uint64) (n int, err error) {
	switch self.StreamType {
	case StreamTypeAdtsAAC:
		var config aacparser.MPEG4AudioConfig
		for len(payload) > 0 {
			var hdrlen, framelen int
			if config, hdrlen, framelen, _, err = aacparser.ParseADTSHeader(payload); err != nil {
				return
			}
			if self.CodecData == nil {
				if self.CodecData, err = aacparser.NewCodecDataFromMPEG4AudioConfig(config); err != nil {
					log.Printf("Parse audio CodecData failed: %v\n", err)
					return
				}
				log.Printf("Parse audio CodecData ok\n")
				self.BReady = true
			}
			self.addPacket(payload[hdrlen:framelen], true, pts)
			n++
			payload = payload[framelen:]
		}

	case StreamTypeH264:
		nalus, _ := h264parser.SplitNALUs(payload)
		var sps, pps []byte
		for _, nalu := range nalus {
			if len(nalu) > 0 {
				var iskeyframe bool = false
				naltype := nalu[0] & 0x1f
				if naltype == 7 || naltype == 8 || naltype == 5 {
					iskeyframe = true
				}
				switch {
				case naltype == 7:
					sps = nalu
				case naltype == 8:
					pps = nalu
				case h264parser.IsDataNALU(nalu):
					// raw nalu to avcc
					if self.CodecData != nil {
						b := make([]byte, 4+len(nalu))
						pio.PutU32BE(b[0:4], uint32(len(nalu)))
						copy(b[4:], nalu)
						self.addPacket(b, iskeyframe, pts)
						n++
					}
				}
			}
		}
		if self.CodecData == nil && len(sps) > 0 && len(pps) > 0 {
			if self.CodecData, err = h264parser.NewCodecDataFromSPSAndPPS(sps, pps); err != nil {
				log.Printf("Parse video CodecData failed: %v\n", err)
				return
			}
			log.Printf("Parse video CodecData ok\n")
			self.BReady = true
		}
	}

	return
}

// func (self *Demuxer) WriteMediaData(StreamType uint8, data []byte, pts uint64) (n int, err error) {
func (self *Demuxer) WriteMediaData(packet Packet) (n int, err error) {
	for _, stream := range self.streams {
		var i int
		if stream.StreamType != packet.Datatype {
			continue
		}
		if i, err = stream.PackMediaData(packet.Data, packet.Pts); err != nil {
			return
		}
		n += i
	}
	return
}
